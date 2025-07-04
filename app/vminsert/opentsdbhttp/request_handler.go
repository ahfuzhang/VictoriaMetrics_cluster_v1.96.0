package opentsdbhttp

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/opentsdbhttp"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/opentsdbhttp/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="opentsdbhttp"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="opentsdbhttp"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="opentsdbhttp"}`)
)

// InsertHandler processes HTTP OpenTSDB put requests.
// See http://opentsdb.net/docs/build/html/api_http/put.html
func InsertHandler(req *http.Request) error {
	path := req.URL.Path
	p, err := httpserver.ParsePath(path)
	if err != nil {
		return fmt.Errorf("cannot parse path %q: %w", path, err)
	}
	if p.Prefix != "insert" {
		// This is not our link.
		return fmt.Errorf("unexpected path requested on HTTP OpenTSDB server: %q", path)
	}
	at, err := auth.NewTokenPossibleMultitenant(p.AuthToken)
	if err != nil {
		return fmt.Errorf("auth error: %w", err)
	}
	switch p.Suffix {
	case "api/put", "opentsdb/api/put":
		extraLabels, err := protoparserutil.GetExtraLabels(req)
		if err != nil {
			return err
		}
		return stream.Parse(req, func(rows []opentsdbhttp.Row) error {
			return insertRows(at, rows, extraLabels)
		})
	default:
		return fmt.Errorf("unexpected path requested on HTTP OpenTSDB server: %q", path)
	}
}

func insertRows(at *auth.Token, rows []opentsdbhttp.Row, extraLabels []prompbmarshal.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)
	ctx.Reset() // This line is required for initializing ctx internals.
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		ctx.AddLabel("", r.Metric)
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabel(tag.Key, tag.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}
		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		atLocal := ctx.GetLocalAuthToken(at)
		if err := ctx.WriteDataPoint(atLocal, ctx.Labels, r.Timestamp, r.Value); err != nil {
			return err
		}
		perTenantRows[*atLocal]++
	}
	rowsInserted.Add(len(rows))
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}
