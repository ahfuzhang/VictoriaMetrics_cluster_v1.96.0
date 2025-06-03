#include "textflag.h"

// func tokenToBitmap(s string, asciiTable *uint8, outBitmap1 *uint8, outBitmap2 *uint8)
TEXT ·tokenToBitmap(SB), NOSPLIT | NOFRAME, $0-40
    // frame length:  0
    // args size:  40 bytes
    //   s 16 bytes
    //   asciiTable 8
    //   outBitmap1 8
    //   outBitmap2 8
    // return value size: 0
    MOVQ inPtr+0(FP), R8  // start
    MOVQ inLen+8(FP), R9  // string length
    // variables
    MOVQ R9, R10
    ANDQ $31, R10  // tailLen = length & 31
    MOVQ R9, R11
    SUBQ R10, R11  // r11 = length - tailLen
    MOVQ R8, R10
    ADDQ R11, R10  // r10 = start + align_32_end
align_32:
    TESTQ R8, R10  // if start==align_32_end then goto align_32_end
    JE align_32_end
    //

    //
    JMP align_32
align_32_end:
