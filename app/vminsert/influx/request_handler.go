package influx

import (
	"flag"
	"io"
	"net/http"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/influx"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/influx/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeserieslimits"
	"github.com/VictoriaMetrics/metrics"
)

var (
	measurementFieldSeparator = flag.String("influxMeasurementFieldSeparator", "_", "Separator for '{measurement}{separator}{field_name}' metric name when inserted via InfluxDB line protocol")
	skipSingleField           = flag.Bool("influxSkipSingleField", false, "Uses '{measurement}' instead of '{measurement}{separator}{field_name}' for metric name if InfluxDB line contains only a single field")
	skipMeasurement           = flag.Bool("influxSkipMeasurement", false, "Uses '{field_name}' as a metric name while ignoring '{measurement}' and '-influxMeasurementFieldSeparator'")
	dbLabel                   = flag.String("influxDBLabel", "db", "Default label for the DB name sent over '?db={db_name}' query parameter")
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="influx"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="influx"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="influx"}`)
)

// InsertHandlerForReader processes remote write for influx line protocol.
//
// See https://github.com/influxdata/telegraf/tree/master/plugins/inputs/socket_listener/
func InsertHandlerForReader(at *auth.Token, r io.Reader) error {
	return stream.Parse(r, "", true, "", "", func(db string, rows []influx.Row) error {
		return insertRows(at, db, rows, nil)
	})
}

// InsertHandlerForHTTP processes remote write for influx line protocol.
//
// See https://github.com/influxdata/influxdb/blob/4cbdc197b8117fee648d62e2e5be75c6575352f0/tsdb/README.md
func InsertHandlerForHTTP(at *auth.Token, req *http.Request) error {
	extraLabels, err := protoparserutil.GetExtraLabels(req)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	precision := q.Get("precision")
	// Read db tag from https://docs.influxdata.com/influxdb/v1.7/tools/api/#write-http-endpoint
	db := q.Get("db")
	encoding := req.Header.Get("Content-Encoding")
	isStreamMode := req.Header.Get("Stream-Mode") == "1"
	return stream.Parse(req.Body, encoding, isStreamMode, precision, db, func(db string, rows []influx.Row) error {
		return insertRows(at, db, rows, extraLabels)
	})
}

func insertRows(at *auth.Token, db string, rows []influx.Row, extraLabels []prompbmarshal.Label) error {
	ctx := getPushCtx()
	defer putPushCtx(ctx)

	ic := &ctx.Common
	ic.Reset() // This line is required for initializing ic internals.
	rowsTotal := 0
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	hasLimitsEnabled := timeserieslimits.Enabled()
	for i := range rows {
		r := &rows[i]
		rowsTotal += len(r.Fields)
		ic.Labels = ic.Labels[:0]
		hasDBKey := false
		for j := range r.Tags {
			tag := &r.Tags[j]
			if tag.Key == *dbLabel {
				hasDBKey = true
			}
			ic.AddLabel(tag.Key, tag.Value)
		}
		if !hasDBKey {
			ic.AddLabel(*dbLabel, db)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ic.AddLabel(label.Name, label.Value)
		}
		ctx.metricGroupBuf = ctx.metricGroupBuf[:0]
		if !*skipMeasurement {
			ctx.metricGroupBuf = append(ctx.metricGroupBuf, r.Measurement...)
		}
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1139
		skipFieldKey := len(r.Measurement) > 0 && len(r.Fields) == 1 && *skipSingleField
		if len(ctx.metricGroupBuf) > 0 && !skipFieldKey {
			ctx.metricGroupBuf = append(ctx.metricGroupBuf, *measurementFieldSeparator...)
		}
		metricGroupPrefixLen := len(ctx.metricGroupBuf)
		if hasRelabeling {
			ctx.originLabels = append(ctx.originLabels[:0], ic.Labels...)
			for j := range r.Fields {
				f := &r.Fields[j]
				if !skipFieldKey {
					ctx.metricGroupBuf = append(ctx.metricGroupBuf[:metricGroupPrefixLen], f.Key...)
				}
				metricGroup := bytesutil.ToUnsafeString(ctx.metricGroupBuf)
				ic.Labels = append(ic.Labels[:0], ctx.originLabels...)
				ic.AddLabel("", metricGroup)
				if !ic.TryPrepareLabels(hasRelabeling) {
					continue
				}
				atLocal := ic.GetLocalAuthToken(at)
				ic.MetricNameBuf = storage.MarshalMetricNameRaw(ic.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, nil)
				for i := range ic.Labels {
					ic.MetricNameBuf = storage.MarshalMetricLabelRaw(ic.MetricNameBuf, &ic.Labels[i])
				}
				storageNodeIdx := ic.GetStorageNodeIdx(atLocal, ic.Labels)
				if err := ic.WriteDataPointExt(storageNodeIdx, ic.MetricNameBuf, r.Timestamp, f.Value); err != nil {
					return err
				}
				perTenantRows[*atLocal]++
			}
		} else {
			// special case for optimisations below
			// do not call TryPrepareLabels
			// manually apply sort and limits on demand
			ic.SortLabelsIfNeeded()
			if hasLimitsEnabled {
				if timeserieslimits.IsExceeding(ic.Labels) {
					continue
				}
			}
			atLocal := ic.GetLocalAuthToken(at)
			ic.MetricNameBuf = storage.MarshalMetricNameRaw(ic.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, ic.Labels)
			metricNameBufLen := len(ic.MetricNameBuf)
			labelsLen := len(ic.Labels)
			for j := range r.Fields {
				f := &r.Fields[j]
				if !skipFieldKey {
					ctx.metricGroupBuf = append(ctx.metricGroupBuf[:metricGroupPrefixLen], f.Key...)
				}
				metricGroup := bytesutil.ToUnsafeString(ctx.metricGroupBuf)
				ic.Labels = ic.Labels[:labelsLen]
				ic.AddLabel("", metricGroup)
				if hasLimitsEnabled {
					if timeserieslimits.IsExceeding(ic.Labels[len(ic.Labels)-1:]) {
						continue
					}
				}
				ic.MetricNameBuf = ic.MetricNameBuf[:metricNameBufLen]
				ic.MetricNameBuf = storage.MarshalMetricLabelRaw(ic.MetricNameBuf, &ic.Labels[len(ic.Labels)-1])
				storageNodeIdx := ic.GetStorageNodeIdx(atLocal, ic.Labels)
				if err := ic.WriteDataPointExt(storageNodeIdx, ic.MetricNameBuf, r.Timestamp, f.Value); err != nil {
					return err
				}
				perTenantRows[*atLocal]++
			}
		}
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(rowsTotal))
	return ic.FlushBufs()
}

type pushCtx struct {
	Common         netstorage.InsertCtx
	metricGroupBuf []byte
	originLabels   []prompbmarshal.Label
}

func (ctx *pushCtx) reset() {
	ctx.Common.Reset()
	ctx.metricGroupBuf = ctx.metricGroupBuf[:0]

	originLabels := ctx.originLabels
	for i := range originLabels {
		originLabels[i] = prompbmarshal.Label{}
	}
	ctx.originLabels = originLabels[:0]
}

func getPushCtx() *pushCtx {
	if v := pushCtxPool.Get(); v != nil {
		return v.(*pushCtx)
	}
	return &pushCtx{}
}

func putPushCtx(ctx *pushCtx) {
	ctx.reset()
	pushCtxPool.Put(ctx)
}

var pushCtxPool sync.Pool
