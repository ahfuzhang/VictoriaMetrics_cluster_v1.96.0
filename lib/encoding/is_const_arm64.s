#include "textflag.h"

TEXT ·IsConst(SB), NOSPLIT | RODATA | NOPTR | NOFRAME, $0-32
    // 栈帧长度  0
    // 参数+返回值长度  32 字节
    MOVD ptr+0(FP), R8  // R8 = ptr
    MOVD len+8(FP), R9 // R9 = len
	MOVD cap+16(FP), R10 // R10 = cap
    // 得到第一个值
    MOVD (R8), R13
    ADD $8, R8
    //
    MOVD $1, R12  // 初始化计数器 R12 为 0
loop_start:
    // 在这里执行循环体中的代码
    // ...
    MOVD (R8), R14
    CMP R14, R13  // 检查与第一个值相等否
    BNE not_equal

    ADD $8, R8
    //ADD $1, R12, R12
    ADD $1, R12
    CMP     R9, R12          // 比较计数器 X1 和循环次数 X0
    BLT    loop_start      // 如果 X1 < X0，则跳回到循环开始处
    //
    //MOVD 0xFFFFFFFFFFFFFFFF, R11
    MOVD $1, R11
    MOVB R11,ret+24(FP)
    RET
not_equal:
    MOVD $0, R11
    MOVB R11,ret+24(FP)
    RET

// 参考  https://github.com/golang/go/blob/master/src/cmd/asm/internal/arch/arm64.go
//      https://github.com/golang/go/tree/master/src/cmd/internal/obj/arm64
//      https://pkg.go.dev/cmd/internal/obj/arm64  arm64 汇编的介绍
//      https://www.symbolcrash.com/2021/03/02/go-assembly-on-the-arm64/   arm64 汇编的入门文章
