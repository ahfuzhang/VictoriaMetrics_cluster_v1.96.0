#include <stdio.h>
#include <inttypes.h>
#include <sys/time.h>
#include <stdlib.h>

void MarshalVarInt64s(uint8_t* dst, uint64_t* outLen, int dstBufferLen, int64_t* src, int count){
	for (int i=0; i<count && *outLen+10<dstBufferLen; i++){
		uint64_t cur = (uint64_t)((src[i] << 1) ^ (src[i] >> 63));
		if (cur==0){
			*dst = 0;
			*outLen++;
			dst++;
			continue;
		}
		switch( (64 - __builtin_clzll(cur>>1))/7 ){
			case 0:
				dst[0] = (uint8_t)(cur);
				*outLen++;
				dst++;
				break;
			case 1:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)(cur>>7);
				*outLen+=2;
				dst+=2;
				break;
			case 2:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14));
				*outLen+=3;
				dst+=3;
				break;
			case 3:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21));
				*outLen+=4;
				dst+=4;
				break;
			case 4:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21)|0x80);
				dst[4] = (uint8_t)((cur>>28));
				*outLen+=5;
				dst+=5;
				break;
			case 5:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21)|0x80);
				dst[4] = (uint8_t)((cur>>28)|0x80);
				dst[5] = (uint8_t)((cur>>35));
				*outLen+=6;
				dst+=6;
				break;
			case 6:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21)|0x80);
				dst[4] = (uint8_t)((cur>>28)|0x80);
				dst[5] = (uint8_t)((cur>>35)|0x80);
				dst[6] = (uint8_t)((cur>>42));
				*outLen+=7;
				dst+=7;
				break;
			case 7:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21)|0x80);
				dst[4] = (uint8_t)((cur>>28)|0x80);
				dst[5] = (uint8_t)((cur>>35)|0x80);
				dst[6] = (uint8_t)((cur>>42)|0x80);
				dst[7] = (uint8_t)((cur>>49));
				*outLen+=8;
				dst+=8;
				break;
			case 8:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21)|0x80);
				dst[4] = (uint8_t)((cur>>28)|0x80);
				dst[5] = (uint8_t)((cur>>35)|0x80);
				dst[6] = (uint8_t)((cur>>42)|0x80);
				dst[7] = (uint8_t)((cur>>49)|0x80);
				dst[8] = (uint8_t)((cur>>56));
				*outLen+=9;
				dst+=9;
				break;
			case 9:
				dst[0] = (uint8_t)(cur|0x80);
				dst[1] = (uint8_t)((cur>>7)|0x80);
				dst[2] = (uint8_t)((cur>>14)|0x80);
				dst[3] = (uint8_t)((cur>>21)|0x80);
				dst[4] = (uint8_t)((cur>>28)|0x80);
				dst[5] = (uint8_t)((cur>>35)|0x80);
				dst[6] = (uint8_t)((cur>>42)|0x80);
				dst[7] = (uint8_t)((cur>>49)|0x80);
				dst[8] = (uint8_t)((cur>>56)|0x80);
				dst[9] = (uint8_t)((cur>>63));
				*outLen+=10;
				dst+=10;
				break;
		}
	}
}

#ifndef NO_MAIN

const uint64_t UintRange7Bit  = (uint64_t)1 << 7;
const uint64_t UintRange14Bit  = (uint64_t)1 << 14;
const uint64_t UintRange21Bit  = (uint64_t)1 << 21;
const uint64_t UintRange28Bit  = (uint64_t)1 << 28;
const uint64_t UintRange35Bit  = (uint64_t)1 << 35;
const uint64_t UintRange42Bit  = (uint64_t)1 << 42;
const uint64_t UintRange49Bit  = (uint64_t)1 << 49;
const uint64_t UintRange56Bit  = (uint64_t)1 << 56;
const uint64_t UintRange63Bit  = (uint64_t)1 << 63;


int main(){
	uint64_t datas[] = {
		0,
		UintRange7Bit - 1, UintRange7Bit,
		UintRange14Bit - 1, UintRange14Bit,
		UintRange21Bit - 1, UintRange21Bit,
		UintRange28Bit - 1, UintRange28Bit,
		UintRange35Bit - 1, UintRange35Bit,
		UintRange42Bit - 1, UintRange42Bit,
		UintRange49Bit - 1, UintRange49Bit,
		UintRange56Bit - 1, UintRange56Bit,
		UintRange63Bit - 1, UintRange63Bit,
		UintRange14Bit + 1, 0xFFFFFFFFFFFFFFFF,
	};
	const int numberCount = 100;
	int count = sizeof(datas)/sizeof(datas[0]);
	int64_t* buf = (uint64_t*)malloc(count*sizeof(uint64_t)*numberCount);
	int idx = 0;
	for (int i=0; i<numberCount; i++){
		for (int j=0; j<count; j++){
			buf[idx] = (int64_t)((datas[j] >> 1) ^ (-(datas[j] & 1)));
			idx++;
		}
	}
	//
	const int runtimes = 1000000;
	struct timeval start, end;
	int dstLen = count*sizeof(uint8_t)*numberCount*10;
	uint8_t* dst = malloc(dstLen);
	uint64_t outlen = 0;
	gettimeofday(&start, NULL);
	for (int i=0; i<runtimes; i++){
		outlen = 0;
		MarshalVarInt64s(dst, &outlen, dstLen, buf, count*numberCount);
	}
	gettimeofday(&end, NULL);
	int total = ((end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec))*1000;
	printf("%ld ns, avg=%.4f ns/op\n", total, (double)total/(double)runtimes);
}
#endif
/*
avg=12.6700 ns/op
avg=4.9100 ns/op
*/
