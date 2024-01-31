#ifndef _VAR_INT64_H_
#define _VAR_INT64_H_

#include <inttypes.h>

typedef struct {
  uint64_t ptr;
  uint64_t len;
  uint64_t cap;
} __attribute__((packed)) SliceHeader;

void marshal_var_int64s(SliceHeader *dst, SliceHeader *src, uint64_t* outCount) {
  uint64_t i = 0;
  uint8_t *out = (uint8_t *)(dst->ptr);
  for (; i < src->len && dst->len + 10 <= dst->cap; i++) {
    int64_t n = ((int64_t *)(src->ptr))[i];
    uint64_t cur = (uint64_t)((n << 1) ^ (n >> 63));
    if (cur == 0) {
      *out = 0;
      dst->len++;
      continue;
    }
    switch ((64 - __builtin_clzll(cur >> 1)) / 7) {
    case 0:
      *out = (uint8_t)(cur);
      dst->len++;
      break;
    case 1:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)(cur >> 7);
      dst->len += 2;
      break;
    case 2:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14));
      dst->len += 3;
      break;
    case 3:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21));
      dst->len += 4;
      break;
    case 4:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21) | 0x80);
      out[4] = (uint8_t)((cur >> 28));
      dst->len += 5;
      break;
    case 5:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21) | 0x80);
      out[4] = (uint8_t)((cur >> 28) | 0x80);
      out[5] = (uint8_t)((cur >> 35));
      dst->len += 6;
      break;
    case 6:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21) | 0x80);
      out[4] = (uint8_t)((cur >> 28) | 0x80);
      out[5] = (uint8_t)((cur >> 35) | 0x80);
      out[6] = (uint8_t)((cur >> 42));
      dst->len += 7;
      break;
    case 7:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21) | 0x80);
      out[4] = (uint8_t)((cur >> 28) | 0x80);
      out[5] = (uint8_t)((cur >> 35) | 0x80);
      out[6] = (uint8_t)((cur >> 42) | 0x80);
      out[7] = (uint8_t)((cur >> 49));
      dst->len += 8;
      break;
    case 8:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21) | 0x80);
      out[4] = (uint8_t)((cur >> 28) | 0x80);
      out[5] = (uint8_t)((cur >> 35) | 0x80);
      out[6] = (uint8_t)((cur >> 42) | 0x80);
      out[7] = (uint8_t)((cur >> 49) | 0x80);
      out[8] = (uint8_t)((cur >> 56));
      dst->len += 9;
      break;
    case 9:
      out[0] = (uint8_t)(cur | 0x80);
      out[1] = (uint8_t)((cur >> 7) | 0x80);
      out[2] = (uint8_t)((cur >> 14) | 0x80);
      out[3] = (uint8_t)((cur >> 21) | 0x80);
      out[4] = (uint8_t)((cur >> 28) | 0x80);
      out[5] = (uint8_t)((cur >> 35) | 0x80);
      out[6] = (uint8_t)((cur >> 42) | 0x80);
      out[7] = (uint8_t)((cur >> 49) | 0x80);
      out[8] = (uint8_t)((cur >> 56) | 0x80);
      out[9] = (uint8_t)((cur >> 63));
      dst->len += 10;
      break;
    }
  }
  *outCount = i;
}

#endif
