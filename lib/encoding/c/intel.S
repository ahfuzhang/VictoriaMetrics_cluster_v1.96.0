	.section	__TEXT,__text,regular,pure_instructions
	.build_version macos, 14, 0	sdk_version 14, 2
	.file	1 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/_types" "_uint64_t.h"
	.file	2 "/Users/fuchunzhang/code/github.com/ahfuzhang/VictoriaMetrics_cluster_v1.96.0/lib/encoding/c" "marshal_var_int64s.c"
	.file	3 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/_types" "_uint8_t.h"
	.file	4 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/sys/_types" "_int64_t.h"
	.section	__TEXT,__literal16,16byte_literals
	.p2align	4, 0x0                          ; -- Begin function MarshalVarInt64s
lCPI0_0:
	.quad	-21                             ; 0xffffffffffffffeb
	.quad	-28                             ; 0xffffffffffffffe4
lCPI0_1:
	.quad	-7                              ; 0xfffffffffffffff9
	.quad	-14                             ; 0xfffffffffffffff2
lCPI0_2:
	.quad	-49                             ; 0xffffffffffffffcf
	.quad	-56                             ; 0xffffffffffffffc8
lCPI0_3:
	.quad	-35                             ; 0xffffffffffffffdd
	.quad	-42                             ; 0xffffffffffffffd6
	.section	__TEXT,__text,regular,pure_instructions
	.globl	_MarshalVarInt64s
	.p2align	2



_MarshalVarInt64s:                      ; @MarshalVarInt64s  // 函数开始
Lfunc_begin0:
	.loc	2 6 0                           ; marshal_var_int64s.c:6:0
	.cfi_startproc
; %bb.0:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:src <- $x3
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
                                        ; kill: def $w2 killed $w2 def $x2
	;DEBUG_VALUE: i <- 0
	.loc	2 7 24 prologue_end             ; marshal_var_int64s.c:7:24
	cmp	w4, #1
	b.lt	LBB0_19
Ltmp0:
; %bb.1:
	;DEBUG_VALUE: i <- 0
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:src <- $x3
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 24 is_stmt 0                ; marshal_var_int64s.c:0:24
	sxtw	x8, w2  // SXTW 指令全称为 "Sign Extend Word"。这条指令的作用是将一个 32 位的有符号整数（word）扩展为 64 位的有符号整数。
	mov	w9, #64
	mov	w10, #37
Lloh0:
	adrp	x11, lCPI0_0@PAGE
Lloh1:
	ldr	q0, [x11, lCPI0_0@PAGEOFF]
Lloh2:
	adrp	x11, lCPI0_1@PAGE
Lloh3:
	ldr	q1, [x11, lCPI0_1@PAGEOFF]
	.loc	2 7 17                          ; marshal_var_int64s.c:7:17
	mov	w11, w4
Lloh4:
	adrp	x12, lCPI0_2@PAGE
Lloh5:
	ldr	q2, [x12, lCPI0_2@PAGEOFF]
Lloh6:
	adrp	x12, lCPI0_3@PAGE
Lloh7:
	ldr	q3, [x12, lCPI0_3@PAGEOFF]
	movi.8b	v4, #128
Lloh8:
	adrp	x12, lJTI0_0@PAGE
Lloh9:
	add	x12, x12, lJTI0_0@PAGEOFF
	b	LBB0_5
Ltmp1:
LBB0_2:                                 ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 10 9 is_stmt 1                ; marshal_var_int64s.c:10:9
	strb	wzr, [x0], #1
Ltmp2:
LBB0_3:                                 ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	.loc	2 0 0 is_stmt 0                 ; marshal_var_int64s.c:0:0
	add	x1, x1, #8
Ltmp3:
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
LBB0_4:                                 ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: i <- [DW_OP_LLVM_arg 0, DW_OP_LLVM_arg 0, DW_OP_minus, DW_OP_consts 8, DW_OP_div, DW_OP_consts 1, DW_OP_plus, DW_OP_stack_value] undef
	.loc	2 7 17 is_stmt 1                ; marshal_var_int64s.c:7:17
	add	x3, x3, #8
Ltmp4:
	;DEBUG_VALUE: MarshalVarInt64s:src <- [DW_OP_LLVM_entry_value 1] $x3
	subs	x11, x11, #1
	.loc	2 7 24 is_stmt 0                ; marshal_var_int64s.c:7:24
	b.eq	LBB0_19
Ltmp5:
LBB0_5:                                 ; =>This Inner Loop Header: Depth=1
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: i <- [DW_OP_LLVM_arg 0, DW_OP_LLVM_arg 0, DW_OP_minus, DW_OP_consts 8, DW_OP_div, DW_OP_stack_value] undef
	.loc	2 7 27                          ; marshal_var_int64s.c:7:27
	ldr	x13, [x1]
	.loc	2 7 34                          ; marshal_var_int64s.c:7:34
	add	x13, x13, #10
Ltmp6:
	.loc	2 7 2                           ; marshal_var_int64s.c:7:2
	cmp	x13, x8
	b.hs	LBB0_19
Ltmp7:
; %bb.6:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 8 30 is_stmt 1                ; marshal_var_int64s.c:8:30
	ldr	x13, [x3]
	.loc	2 8 37 is_stmt 0                ; marshal_var_int64s.c:8:37
	lsl	x14, x13, #1
	.loc	2 8 43                          ; marshal_var_int64s.c:8:43
	eor	x13, x14, x13, asr #63
Ltmp8:
	;DEBUG_VALUE: cur <- $x13
	.loc	2 9 7 is_stmt 1                 ; marshal_var_int64s.c:9:7
	cbz	x13, LBB0_2
Ltmp9:
; %bb.7:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 15 36                         ; marshal_var_int64s.c:15:36
	lsr	x14, x13, #1
	.loc	2 15 17 is_stmt 0               ; marshal_var_int64s.c:15:17
	clz	x14, x14
	.loc	2 15 15                         ; marshal_var_int64s.c:15:15
	sub	w14, w9, w14
	.loc	2 15 41                         ; marshal_var_int64s.c:15:41
	and	w15, w14, #0xff
	mul	w15, w15, w10
	lsr	w15, w15, #8
	sub	w14, w14, w15
	and	w14, w14, #0xfe
	add	w14, w15, w14, lsr #1
	lsr	w14, w14, #2
	.loc	2 15 3                          ; marshal_var_int64s.c:15:3
	cmp	w14, #9
	b.hi	LBB0_4
Ltmp10:
; %bb.8:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 3                           ; marshal_var_int64s.c:0:3
	adr	x15, LBB0_9
	ldrb	w16, [x12, x14]
	add	x15, x15, x16, lsl #2
	br	x15
Ltmp11:
LBB0_9:                                 ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 17 12 is_stmt 1               ; marshal_var_int64s.c:17:12
	strb	w13, [x0], #1
Ltmp12:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 12 is_stmt 0                ; marshal_var_int64s.c:0:12
	b	LBB0_3
Ltmp13:
LBB0_10:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 22 14 is_stmt 1               ; marshal_var_int64s.c:22:14
	orr	w14, w13, #0x80
	.loc	2 22 12 is_stmt 0               ; marshal_var_int64s.c:22:12
	strb	w14, [x0]
	.loc	2 23 27 is_stmt 1               ; marshal_var_int64s.c:23:27
	lsr	x13, x13, #7
Ltmp14:
	.loc	2 23 12 is_stmt 0               ; marshal_var_int64s.c:23:12
	strb	w13, [x0, #1]
	.loc	2 24 12 is_stmt 1               ; marshal_var_int64s.c:24:12
	ldr	x13, [x1]
	add	x13, x13, #2
	str	x13, [x1]
	.loc	2 25 8                          ; marshal_var_int64s.c:25:8
	add	x0, x0, #2
Ltmp15:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp16:
LBB0_11:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 28 14 is_stmt 1               ; marshal_var_int64s.c:28:14
	orr	w14, w13, #0x80
	.loc	2 28 12 is_stmt 0               ; marshal_var_int64s.c:28:12
	strb	w14, [x0]
	.loc	2 29 14 is_stmt 1               ; marshal_var_int64s.c:29:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 29 12 is_stmt 0               ; marshal_var_int64s.c:29:12
	strb	w14, [x0, #1]
	.loc	2 30 28 is_stmt 1               ; marshal_var_int64s.c:30:28
	lsr	x13, x13, #14
Ltmp17:
	.loc	2 30 12 is_stmt 0               ; marshal_var_int64s.c:30:12
	strb	w13, [x0, #2]
	.loc	2 31 12 is_stmt 1               ; marshal_var_int64s.c:31:12
	ldr	x13, [x1]
	add	x13, x13, #3
	str	x13, [x1]
	.loc	2 32 8                          ; marshal_var_int64s.c:32:8
	add	x0, x0, #3
Ltmp18:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp19:
LBB0_12:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 35 14 is_stmt 1               ; marshal_var_int64s.c:35:14
	orr	w14, w13, #0x80
	.loc	2 35 12 is_stmt 0               ; marshal_var_int64s.c:35:12
	strb	w14, [x0]
	.loc	2 36 14 is_stmt 1               ; marshal_var_int64s.c:36:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 36 12 is_stmt 0               ; marshal_var_int64s.c:36:12
	strb	w14, [x0, #1]
	.loc	2 37 14 is_stmt 1               ; marshal_var_int64s.c:37:14
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	.loc	2 37 12 is_stmt 0               ; marshal_var_int64s.c:37:12
	strb	w14, [x0, #2]
	.loc	2 38 28 is_stmt 1               ; marshal_var_int64s.c:38:28
	lsr	x13, x13, #21
Ltmp20:
	.loc	2 38 12 is_stmt 0               ; marshal_var_int64s.c:38:12
	strb	w13, [x0, #3]
	.loc	2 39 12 is_stmt 1               ; marshal_var_int64s.c:39:12
	ldr	x13, [x1]
	add	x13, x13, #4
	str	x13, [x1]
	.loc	2 40 8                          ; marshal_var_int64s.c:40:8
	add	x0, x0, #4
Ltmp21:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp22:
LBB0_13:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 43 14 is_stmt 1               ; marshal_var_int64s.c:43:14
	orr	w14, w13, #0x80
	.loc	2 43 12 is_stmt 0               ; marshal_var_int64s.c:43:12
	strb	w14, [x0]
	.loc	2 44 14 is_stmt 1               ; marshal_var_int64s.c:44:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 44 12 is_stmt 0               ; marshal_var_int64s.c:44:12
	strb	w14, [x0, #1]
	.loc	2 45 14 is_stmt 1               ; marshal_var_int64s.c:45:14
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	.loc	2 45 12 is_stmt 0               ; marshal_var_int64s.c:45:12
	strb	w14, [x0, #2]
	.loc	2 46 14 is_stmt 1               ; marshal_var_int64s.c:46:14
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	.loc	2 46 12 is_stmt 0               ; marshal_var_int64s.c:46:12
	strb	w14, [x0, #3]
	.loc	2 47 28 is_stmt 1               ; marshal_var_int64s.c:47:28
	lsr	x13, x13, #28
Ltmp23:
	.loc	2 47 12 is_stmt 0               ; marshal_var_int64s.c:47:12
	strb	w13, [x0, #4]
	.loc	2 48 12 is_stmt 1               ; marshal_var_int64s.c:48:12
	ldr	x13, [x1]
	add	x13, x13, #5
	str	x13, [x1]
	.loc	2 49 8                          ; marshal_var_int64s.c:49:8
	add	x0, x0, #5
Ltmp24:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp25:
LBB0_14:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 52 14 is_stmt 1               ; marshal_var_int64s.c:52:14
	orr	w14, w13, #0x80
	.loc	2 52 12 is_stmt 0               ; marshal_var_int64s.c:52:12
	strb	w14, [x0]
	.loc	2 53 14 is_stmt 1               ; marshal_var_int64s.c:53:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 53 12 is_stmt 0               ; marshal_var_int64s.c:53:12
	strb	w14, [x0, #1]
	.loc	2 54 14 is_stmt 1               ; marshal_var_int64s.c:54:14
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	.loc	2 54 12 is_stmt 0               ; marshal_var_int64s.c:54:12
	strb	w14, [x0, #2]
	.loc	2 55 14 is_stmt 1               ; marshal_var_int64s.c:55:14
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	.loc	2 55 12 is_stmt 0               ; marshal_var_int64s.c:55:12
	strb	w14, [x0, #3]
	.loc	2 56 28 is_stmt 1               ; marshal_var_int64s.c:56:28
	lsr	x14, x13, #28
	.loc	2 56 14 is_stmt 0               ; marshal_var_int64s.c:56:14
	orr	w14, w14, #0x80
	.loc	2 56 12                         ; marshal_var_int64s.c:56:12
	strb	w14, [x0, #4]
	.loc	2 57 28 is_stmt 1               ; marshal_var_int64s.c:57:28
	lsr	x13, x13, #35
Ltmp26:
	.loc	2 57 12 is_stmt 0               ; marshal_var_int64s.c:57:12
	strb	w13, [x0, #5]
	.loc	2 58 12 is_stmt 1               ; marshal_var_int64s.c:58:12
	ldr	x13, [x1]
	add	x13, x13, #6
	str	x13, [x1]
	.loc	2 59 8                          ; marshal_var_int64s.c:59:8
	add	x0, x0, #6
Ltmp27:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp28:
LBB0_15:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 62 14 is_stmt 1               ; marshal_var_int64s.c:62:14
	orr	w14, w13, #0x80
	.loc	2 62 12 is_stmt 0               ; marshal_var_int64s.c:62:12
	strb	w14, [x0]
	.loc	2 63 14 is_stmt 1               ; marshal_var_int64s.c:63:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 63 12 is_stmt 0               ; marshal_var_int64s.c:63:12
	strb	w14, [x0, #1]
	.loc	2 64 14 is_stmt 1               ; marshal_var_int64s.c:64:14
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	.loc	2 64 12 is_stmt 0               ; marshal_var_int64s.c:64:12
	strb	w14, [x0, #2]
	.loc	2 65 14 is_stmt 1               ; marshal_var_int64s.c:65:14
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	.loc	2 65 12 is_stmt 0               ; marshal_var_int64s.c:65:12
	strb	w14, [x0, #3]
	.loc	2 66 28 is_stmt 1               ; marshal_var_int64s.c:66:28
	lsr	x14, x13, #28
	.loc	2 66 14 is_stmt 0               ; marshal_var_int64s.c:66:14
	orr	w14, w14, #0x80
	.loc	2 66 12                         ; marshal_var_int64s.c:66:12
	strb	w14, [x0, #4]
	.loc	2 67 28 is_stmt 1               ; marshal_var_int64s.c:67:28
	lsr	x14, x13, #35
	.loc	2 67 14 is_stmt 0               ; marshal_var_int64s.c:67:14
	orr	w14, w14, #0x80
	.loc	2 67 12                         ; marshal_var_int64s.c:67:12
	strb	w14, [x0, #5]
	.loc	2 68 28 is_stmt 1               ; marshal_var_int64s.c:68:28
	lsr	x13, x13, #42
Ltmp29:
	.loc	2 68 12 is_stmt 0               ; marshal_var_int64s.c:68:12
	strb	w13, [x0, #6]
	.loc	2 69 12 is_stmt 1               ; marshal_var_int64s.c:69:12
	ldr	x13, [x1]
	add	x13, x13, #7
	str	x13, [x1]
	.loc	2 70 8                          ; marshal_var_int64s.c:70:8
	add	x0, x0, #7
Ltmp30:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp31:
LBB0_16:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 73 14 is_stmt 1               ; marshal_var_int64s.c:73:14
	orr	w14, w13, #0x80
	.loc	2 73 12 is_stmt 0               ; marshal_var_int64s.c:73:12
	strb	w14, [x0]
	.loc	2 74 14 is_stmt 1               ; marshal_var_int64s.c:74:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 74 12 is_stmt 0               ; marshal_var_int64s.c:74:12
	strb	w14, [x0, #1]
	.loc	2 75 14 is_stmt 1               ; marshal_var_int64s.c:75:14
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	.loc	2 75 12 is_stmt 0               ; marshal_var_int64s.c:75:12
	strb	w14, [x0, #2]
	.loc	2 76 14 is_stmt 1               ; marshal_var_int64s.c:76:14
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	.loc	2 76 12 is_stmt 0               ; marshal_var_int64s.c:76:12
	strb	w14, [x0, #3]
	.loc	2 77 28 is_stmt 1               ; marshal_var_int64s.c:77:28
	lsr	x14, x13, #28
	.loc	2 77 14 is_stmt 0               ; marshal_var_int64s.c:77:14
	orr	w14, w14, #0x80
	.loc	2 77 12                         ; marshal_var_int64s.c:77:12
	strb	w14, [x0, #4]
	.loc	2 78 28 is_stmt 1               ; marshal_var_int64s.c:78:28
	lsr	x14, x13, #35
	.loc	2 78 14 is_stmt 0               ; marshal_var_int64s.c:78:14
	orr	w14, w14, #0x80
	.loc	2 78 12                         ; marshal_var_int64s.c:78:12
	strb	w14, [x0, #5]
	.loc	2 79 28 is_stmt 1               ; marshal_var_int64s.c:79:28
	lsr	x14, x13, #42
	.loc	2 79 14 is_stmt 0               ; marshal_var_int64s.c:79:14
	orr	w14, w14, #0x80
	.loc	2 79 12                         ; marshal_var_int64s.c:79:12
	strb	w14, [x0, #6]
	.loc	2 80 28 is_stmt 1               ; marshal_var_int64s.c:80:28
	lsr	x13, x13, #49
Ltmp32:
	.loc	2 80 12 is_stmt 0               ; marshal_var_int64s.c:80:12
	strb	w13, [x0, #7]
	.loc	2 81 12 is_stmt 1               ; marshal_var_int64s.c:81:12
	ldr	x13, [x1]
	add	x13, x13, #8
	str	x13, [x1]
	.loc	2 82 8                          ; marshal_var_int64s.c:82:8
	add	x0, x0, #8
Ltmp33:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp34:
LBB0_17:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 85 14 is_stmt 1               ; marshal_var_int64s.c:85:14
	orr	w14, w13, #0x80
	.loc	2 85 12 is_stmt 0               ; marshal_var_int64s.c:85:12
	strb	w14, [x0]
	.loc	2 86 14 is_stmt 1               ; marshal_var_int64s.c:86:14
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	.loc	2 86 12 is_stmt 0               ; marshal_var_int64s.c:86:12
	strb	w14, [x0, #1]
	.loc	2 87 14 is_stmt 1               ; marshal_var_int64s.c:87:14
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	.loc	2 87 12 is_stmt 0               ; marshal_var_int64s.c:87:12
	strb	w14, [x0, #2]
	.loc	2 88 14 is_stmt 1               ; marshal_var_int64s.c:88:14
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	.loc	2 88 12 is_stmt 0               ; marshal_var_int64s.c:88:12
	strb	w14, [x0, #3]
	.loc	2 89 28 is_stmt 1               ; marshal_var_int64s.c:89:28
	lsr	x14, x13, #28
	.loc	2 89 14 is_stmt 0               ; marshal_var_int64s.c:89:14
	orr	w14, w14, #0x80
	.loc	2 89 12                         ; marshal_var_int64s.c:89:12
	strb	w14, [x0, #4]
	.loc	2 90 28 is_stmt 1               ; marshal_var_int64s.c:90:28
	lsr	x14, x13, #35
	.loc	2 90 14 is_stmt 0               ; marshal_var_int64s.c:90:14
	orr	w14, w14, #0x80
	.loc	2 90 12                         ; marshal_var_int64s.c:90:12
	strb	w14, [x0, #5]
	.loc	2 91 28 is_stmt 1               ; marshal_var_int64s.c:91:28
	lsr	x14, x13, #42
	.loc	2 91 14 is_stmt 0               ; marshal_var_int64s.c:91:14
	orr	w14, w14, #0x80
	.loc	2 91 12                         ; marshal_var_int64s.c:91:12
	strb	w14, [x0, #6]
	.loc	2 92 28 is_stmt 1               ; marshal_var_int64s.c:92:28
	lsr	x14, x13, #49
	.loc	2 92 14 is_stmt 0               ; marshal_var_int64s.c:92:14
	orr	w14, w14, #0x80
	.loc	2 92 12                         ; marshal_var_int64s.c:92:12
	strb	w14, [x0, #7]
	.loc	2 93 28 is_stmt 1               ; marshal_var_int64s.c:93:28
	lsr	x13, x13, #56
Ltmp35:
	.loc	2 93 12 is_stmt 0               ; marshal_var_int64s.c:93:12
	strb	w13, [x0, #8]
	.loc	2 94 12 is_stmt 1               ; marshal_var_int64s.c:94:12
	ldr	x13, [x1]
	add	x13, x13, #9
	str	x13, [x1]
	.loc	2 95 8                          ; marshal_var_int64s.c:95:8
	add	x0, x0, #9
Ltmp36:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp37:
LBB0_18:                                ;   in Loop: Header=BB0_5 Depth=1
	;DEBUG_VALUE: cur <- $x13
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 98 14 is_stmt 1               ; marshal_var_int64s.c:98:14
	orr	w14, w13, #0x80
	.loc	2 98 12 is_stmt 0               ; marshal_var_int64s.c:98:12
	strb	w14, [x0]
	.loc	2 99 28 is_stmt 1               ; marshal_var_int64s.c:99:28
	dup.2d	v5, x13
	ushl.2d	v6, v5, v0
	ushl.2d	v7, v5, v1
	ushl.2d	v16, v5, v2
	ushl.2d	v5, v5, v3
	.loc	2 99 14 is_stmt 0               ; marshal_var_int64s.c:99:14
	uzp1.4s	v5, v5, v16
	uzp1.4s	v6, v7, v6
	uzp1.8h	v5, v6, v5
	xtn.8b	v5, v5
	orr.8b	v5, v5, v4
	.loc	2 99 12                         ; marshal_var_int64s.c:99:12
	stur	d5, [x0, #1]
	.loc	2 107 28 is_stmt 1              ; marshal_var_int64s.c:107:28
	lsr	x13, x13, #63
Ltmp38:
	.loc	2 107 12 is_stmt 0              ; marshal_var_int64s.c:107:12
	strb	w13, [x0, #9]
	.loc	2 108 12 is_stmt 1              ; marshal_var_int64s.c:108:12
	ldr	x13, [x1]
	add	x13, x13, #10
	str	x13, [x1]
	.loc	2 109 8                         ; marshal_var_int64s.c:109:8
	add	x0, x0, #10
Ltmp39:
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 0 8 is_stmt 0                 ; marshal_var_int64s.c:0:8
	b	LBB0_4
Ltmp40:
LBB0_19:
	;DEBUG_VALUE: MarshalVarInt64s:count <- $w4
	;DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $w2
	;DEBUG_VALUE: MarshalVarInt64s:outLen <- $x1
	;DEBUG_VALUE: MarshalVarInt64s:dst <- $x0
	.loc	2 113 1 is_stmt 1               ; marshal_var_int64s.c:113:1
	ret
Ltmp41:
	.loh AdrpAdd	Lloh8, Lloh9
	.loh AdrpAdrp	Lloh6, Lloh8
	.loh AdrpLdr	Lloh6, Lloh7
	.loh AdrpAdrp	Lloh4, Lloh6
	.loh AdrpLdr	Lloh4, Lloh5
	.loh AdrpLdr	Lloh2, Lloh3
	.loh AdrpAdrp	Lloh0, Lloh2
	.loh AdrpLdr	Lloh0, Lloh1
Lfunc_end0:
	.cfi_endproc
	.section	__TEXT,__const
lJTI0_0:
	.byte	(LBB0_9-LBB0_9)>>2
	.byte	(LBB0_10-LBB0_9)>>2
	.byte	(LBB0_11-LBB0_9)>>2
	.byte	(LBB0_12-LBB0_9)>>2
	.byte	(LBB0_13-LBB0_9)>>2
	.byte	(LBB0_14-LBB0_9)>>2
	.byte	(LBB0_15-LBB0_9)>>2
	.byte	(LBB0_16-LBB0_9)>>2
	.byte	(LBB0_17-LBB0_9)>>2
	.byte	(LBB0_18-LBB0_9)>>2
                                        ; -- End function
	.section	__TEXT,__literal16,16byte_literals
	.p2align	4, 0x0                          ; -- Begin function main
lCPI1_0:
	.quad	0                               ; 0x0
	.quad	-64                             ; 0xffffffffffffffc0
lCPI1_1:
	.quad	64                              ; 0x40
	.quad	-8192                           ; 0xffffffffffffe000
lCPI1_2:
	.quad	8192                            ; 0x2000
	.quad	-1048576                        ; 0xfffffffffff00000
lCPI1_3:
	.quad	1048576                         ; 0x100000
	.quad	-134217728                      ; 0xfffffffff8000000
lCPI1_4:
	.quad	134217728                       ; 0x8000000
	.quad	-17179869184                    ; 0xfffffffc00000000
lCPI1_5:
	.quad	17179869184                     ; 0x400000000
	.quad	-2199023255552                  ; 0xfffffe0000000000
lCPI1_6:
	.quad	2199023255552                   ; 0x20000000000
	.quad	-281474976710656                ; 0xffff000000000000
lCPI1_7:
	.quad	281474976710656                 ; 0x1000000000000
	.quad	-36028797018963968              ; 0xff80000000000000
lCPI1_8:
	.quad	36028797018963968               ; 0x80000000000000
	.quad	-4611686018427387904            ; 0xc000000000000000
lCPI1_9:
	.quad	4611686018427387904             ; 0x4000000000000000
	.quad	-8193                           ; 0xffffffffffffdfff
	.section	__TEXT,__text,regular,pure_instructions
	.globl	_main
	.p2align	2
_main:                                  ; @main
Lfunc_begin1:
	.loc	2 126 0                         ; marshal_var_int64s.c:126:0
	.cfi_startproc
; %bb.0:
	sub	sp, sp, #112
	.cfi_def_cfa_offset 112
	stp	x22, x21, [sp, #64]             ; 16-byte Folded Spill
	stp	x20, x19, [sp, #80]             ; 16-byte Folded Spill
	stp	x29, x30, [sp, #96]             ; 16-byte Folded Spill
	add	x29, sp, #96
	.cfi_def_cfa w29, 16
	.cfi_offset w30, -8
	.cfi_offset w29, -16
	.cfi_offset w19, -24
	.cfi_offset w20, -32
	.cfi_offset w21, -40
	.cfi_offset w22, -48
Ltmp42:
	;DEBUG_VALUE: main:numberCount <- 100
	;DEBUG_VALUE: main:count <- 21
	.loc	2 142 28 prologue_end           ; marshal_var_int64s.c:142:28
	mov	w0, #16800
	bl	_malloc
Ltmp43:
	mov	x19, x0
Ltmp44:
	;DEBUG_VALUE: i <- 0
	;DEBUG_VALUE: main:idx <- 0
	;DEBUG_VALUE: main:buf <- $x19
	.loc	2 0 28 is_stmt 0                ; marshal_var_int64s.c:0:28
Lloh10:
	adrp	x8, lCPI1_0@PAGE
Lloh11:
	ldr	q0, [x8, lCPI1_0@PAGEOFF]
Lloh12:
	adrp	x8, lCPI1_1@PAGE
Lloh13:
	ldr	q1, [x8, lCPI1_1@PAGEOFF]
Ltmp45:
	.loc	2 144 2 is_stmt 1               ; marshal_var_int64s.c:144:2
	add	x8, x0, #80
	mov	w9, #100
Lloh14:
	adrp	x10, lCPI1_2@PAGE
Lloh15:
	ldr	q2, [x10, lCPI1_2@PAGEOFF]
Lloh16:
	adrp	x10, lCPI1_3@PAGE
Lloh17:
	ldr	q3, [x10, lCPI1_3@PAGEOFF]
Lloh18:
	adrp	x10, lCPI1_4@PAGE
Lloh19:
	ldr	q4, [x10, lCPI1_4@PAGEOFF]
Lloh20:
	adrp	x10, lCPI1_5@PAGE
Lloh21:
	ldr	q5, [x10, lCPI1_5@PAGEOFF]
Lloh22:
	adrp	x10, lCPI1_6@PAGE
Lloh23:
	ldr	q6, [x10, lCPI1_6@PAGEOFF]
Lloh24:
	adrp	x10, lCPI1_7@PAGE
Lloh25:
	ldr	q7, [x10, lCPI1_7@PAGEOFF]
Lloh26:
	adrp	x10, lCPI1_8@PAGE
Lloh27:
	ldr	q16, [x10, lCPI1_8@PAGEOFF]
Lloh28:
	adrp	x10, lCPI1_9@PAGE
Lloh29:
	ldr	q17, [x10, lCPI1_9@PAGEOFF]
	mov	x10, #-9223372036854775808
Ltmp46:
LBB1_1:                                 ; =>This Inner Loop Header: Depth=1
	;DEBUG_VALUE: main:buf <- $x19
	;DEBUG_VALUE: main:count <- 21
	;DEBUG_VALUE: main:numberCount <- 100
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 1, DW_OP_stack_value] $x8
	;DEBUG_VALUE: i <- [DW_OP_LLVM_arg 0, DW_OP_consts 80, DW_OP_LLVM_arg 1, DW_OP_plus, DW_OP_minus, DW_OP_consts 168, DW_OP_div, DW_OP_stack_value] $x8, $x19
	;DEBUG_VALUE: j <- 1
	.loc	2 146 13                        ; marshal_var_int64s.c:146:13
	stp	q0, q1, [x8, #-80]
Ltmp47:
	;DEBUG_VALUE: j <- 3
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 3, DW_OP_stack_value] $x8
	;DEBUG_VALUE: j <- 5
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 5, DW_OP_stack_value] $x8
	stp	q2, q3, [x8, #-48]
Ltmp48:
	;DEBUG_VALUE: j <- 7
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 7, DW_OP_stack_value] $x8
	;DEBUG_VALUE: j <- 9
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 9, DW_OP_stack_value] $x8
	stp	q4, q5, [x8, #-16]
Ltmp49:
	;DEBUG_VALUE: j <- 11
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 11, DW_OP_stack_value] $x8
	;DEBUG_VALUE: j <- 13
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 13, DW_OP_stack_value] $x8
	stp	q6, q7, [x8, #16]
Ltmp50:
	;DEBUG_VALUE: j <- 15
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 15, DW_OP_stack_value] $x8
	;DEBUG_VALUE: j <- 17
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 17, DW_OP_stack_value] $x8
	stp	q16, q17, [x8, #48]
Ltmp51:
	;DEBUG_VALUE: j <- 19
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 19, DW_OP_stack_value] $x8
	;DEBUG_VALUE: j <- 20
	;DEBUG_VALUE: main:idx <- [DW_OP_plus_uconst 20, DW_OP_stack_value] $x8
	str	x10, [x8, #80]
Ltmp52:
	;DEBUG_VALUE: j <- 21
	;DEBUG_VALUE: i <- [DW_OP_LLVM_arg 0, DW_OP_consts 80, DW_OP_LLVM_arg 1, DW_OP_plus, DW_OP_minus, DW_OP_consts 168, DW_OP_div, DW_OP_consts 1, DW_OP_plus, DW_OP_stack_value] $x8, $x19
	;DEBUG_VALUE: main:idx <- [DW_OP_LLVM_convert 64 7, DW_OP_LLVM_convert 32 7, DW_OP_stack_value] $x8
	.loc	2 144 17                        ; marshal_var_int64s.c:144:17
	add	x8, x8, #168
Ltmp53:
	subs	w9, w9, #1
Ltmp54:
	.loc	2 144 2 is_stmt 0               ; marshal_var_int64s.c:144:2
	b.ne	LBB1_1
Ltmp55:
; %bb.2:
	;DEBUG_VALUE: main:buf <- $x19
	;DEBUG_VALUE: main:count <- 21
	;DEBUG_VALUE: main:numberCount <- 100
	.loc	2 0 2                           ; marshal_var_int64s.c:0:2
	mov	w21, #16960
	movk	w21, #15, lsl #16
Ltmp56:
	;DEBUG_VALUE: main:dstLen <- 21000
	;DEBUG_VALUE: main:runtimes <- 1000000
	.loc	2 154 17 is_stmt 1              ; marshal_var_int64s.c:154:17
	mov	w0, #21000
	bl	_malloc
Ltmp57:
	mov	x20, x0
Ltmp58:
	;DEBUG_VALUE: main:outlen <- 0
	;DEBUG_VALUE: main:dst <- $x20
	.loc	2 156 2                         ; marshal_var_int64s.c:156:2
	add	x0, sp, #48
	mov	x1, #0
	bl	_gettimeofday
Ltmp59:
	;DEBUG_VALUE: i <- 0
	.loc	2 0 2 is_stmt 0                 ; marshal_var_int64s.c:0:2
	mov	w22, #16960
	movk	w22, #15, lsl #16
Ltmp60:
LBB1_3:                                 ; =>This Inner Loop Header: Depth=1
	;DEBUG_VALUE: main:dst <- $x20
	;DEBUG_VALUE: main:runtimes <- 1000000
	;DEBUG_VALUE: main:dstLen <- 21000
	;DEBUG_VALUE: main:buf <- $x19
	;DEBUG_VALUE: main:count <- 21
	;DEBUG_VALUE: main:numberCount <- 100
	;DEBUG_VALUE: main:outlen <- 0
	;DEBUG_VALUE: i <- [DW_OP_consts 1000000, DW_OP_minus, DW_OP_consts 18446744073709551615, DW_OP_div, DW_OP_stack_value] $w22
	.loc	2 158 10 is_stmt 1              ; marshal_var_int64s.c:158:10
	str	xzr, [sp, #24]
Ltmp61:
	;DEBUG_VALUE: main:outlen <- [DW_OP_plus_uconst 24, DW_OP_deref] $sp
	.loc	2 159 3                         ; marshal_var_int64s.c:159:3
	add	x1, sp, #24
	mov	x0, x20
	mov	w2, #21000
	mov	x3, x19
	mov	w4, #2100
	bl	_MarshalVarInt64s
Ltmp62:
	;DEBUG_VALUE: i <- [DW_OP_consts 1000000, DW_OP_minus, DW_OP_consts 18446744073709551615, DW_OP_div, DW_OP_consts 1, DW_OP_plus, DW_OP_stack_value] $w22
	.loc	2 157 17                        ; marshal_var_int64s.c:157:17
	subs	w22, w22, #1
Ltmp63:
	.loc	2 157 2 is_stmt 0               ; marshal_var_int64s.c:157:2
	b.ne	LBB1_3
Ltmp64:
; %bb.4:
	;DEBUG_VALUE: main:outlen <- [DW_OP_plus_uconst 24, DW_OP_deref] $sp
	;DEBUG_VALUE: main:dst <- $x20
	;DEBUG_VALUE: main:runtimes <- 1000000
	;DEBUG_VALUE: main:dstLen <- 21000
	;DEBUG_VALUE: main:buf <- $x19
	;DEBUG_VALUE: main:count <- 21
	;DEBUG_VALUE: main:numberCount <- 100
	.loc	2 161 2 is_stmt 1               ; marshal_var_int64s.c:161:2
	add	x0, sp, #32
	mov	x1, #0
	bl	_gettimeofday
Ltmp65:
	.loc	2 162 20                        ; marshal_var_int64s.c:162:20
	ldr	w8, [sp, #32]
	.loc	2 162 54 is_stmt 0              ; marshal_var_int64s.c:162:54
	ldr	w9, [sp, #40]
	.loc	2 162 33                        ; marshal_var_int64s.c:162:33
	ldr	w10, [sp, #48]
	.loc	2 162 68                        ; marshal_var_int64s.c:162:68
	ldr	w11, [sp, #56]
	.loc	2 162 26                        ; marshal_var_int64s.c:162:26
	sub	w8, w8, w10
	.loc	2 162 61                        ; marshal_var_int64s.c:162:61
	sub	w9, w9, w11
	.loc	2 162 14                        ; marshal_var_int64s.c:162:14
	madd	w8, w8, w21, w9
	mov	w9, #1000
	mul	w8, w8, w9
Ltmp66:
	;DEBUG_VALUE: main:total <- $w8
	.loc	2 163 44 is_stmt 1              ; marshal_var_int64s.c:163:44
	scvtf	d0, w8
	mov	x9, #145685290680320
	movk	x9, #16686, lsl #48
	fmov	d1, x9
	.loc	2 163 57 is_stmt 0              ; marshal_var_int64s.c:163:57
	fdiv	d0, d0, d1
	.loc	2 163 2                         ; marshal_var_int64s.c:163:2
	str	x8, [sp]
	str	d0, [sp, #8]
Lloh30:
	adrp	x0, l_.str@PAGE
Lloh31:
	add	x0, x0, l_.str@PAGEOFF
	bl	_printf
Ltmp67:
	.loc	2 164 1 is_stmt 1               ; marshal_var_int64s.c:164:1
	mov	w0, #0
	ldp	x29, x30, [sp, #96]             ; 16-byte Folded Reload
	ldp	x20, x19, [sp, #80]             ; 16-byte Folded Reload
Ltmp68:
	ldp	x22, x21, [sp, #64]             ; 16-byte Folded Reload
	add	sp, sp, #112
Ltmp69:
	ret
Ltmp70:
	.loh AdrpLdr	Lloh28, Lloh29
	.loh AdrpAdrp	Lloh26, Lloh28
	.loh AdrpLdr	Lloh26, Lloh27
	.loh AdrpAdrp	Lloh24, Lloh26
	.loh AdrpLdr	Lloh24, Lloh25
	.loh AdrpAdrp	Lloh22, Lloh24
	.loh AdrpLdr	Lloh22, Lloh23
	.loh AdrpAdrp	Lloh20, Lloh22
	.loh AdrpLdr	Lloh20, Lloh21
	.loh AdrpAdrp	Lloh18, Lloh20
	.loh AdrpLdr	Lloh18, Lloh19
	.loh AdrpAdrp	Lloh16, Lloh18
	.loh AdrpLdr	Lloh16, Lloh17
	.loh AdrpAdrp	Lloh14, Lloh16
	.loh AdrpLdr	Lloh14, Lloh15
	.loh AdrpLdr	Lloh12, Lloh13
	.loh AdrpAdrp	Lloh10, Lloh12
	.loh AdrpLdr	Lloh10, Lloh11
	.loh AdrpAdd	Lloh30, Lloh31
Lfunc_end1:
	.cfi_endproc
	.file	5 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/sys" "time.h"
	.file	6 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/arm" "_types.h"
	.file	7 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/sys/_types" "_timeval.h"
	.file	8 "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/sys" "_types.h"
                                        ; -- End function
	.section	__TEXT,__const
	.globl	_UintRange7Bit                  ; @UintRange7Bit
	.p2align	3, 0x0
_UintRange7Bit:
	.quad	128                             ; 0x80

	.globl	_UintRange14Bit                 ; @UintRange14Bit
	.p2align	3, 0x0
_UintRange14Bit:
	.quad	16384                           ; 0x4000

	.globl	_UintRange21Bit                 ; @UintRange21Bit
	.p2align	3, 0x0
_UintRange21Bit:
	.quad	2097152                         ; 0x200000

	.globl	_UintRange28Bit                 ; @UintRange28Bit
	.p2align	3, 0x0
_UintRange28Bit:
	.quad	268435456                       ; 0x10000000

	.globl	_UintRange35Bit                 ; @UintRange35Bit
	.p2align	3, 0x0
_UintRange35Bit:
	.quad	34359738368                     ; 0x800000000

	.globl	_UintRange42Bit                 ; @UintRange42Bit
	.p2align	3, 0x0
_UintRange42Bit:
	.quad	4398046511104                   ; 0x40000000000

	.globl	_UintRange49Bit                 ; @UintRange49Bit
	.p2align	3, 0x0
_UintRange49Bit:
	.quad	562949953421312                 ; 0x2000000000000

	.globl	_UintRange56Bit                 ; @UintRange56Bit
	.p2align	3, 0x0
_UintRange56Bit:
	.quad	72057594037927936               ; 0x100000000000000

	.globl	_UintRange63Bit                 ; @UintRange63Bit
	.p2align	3, 0x0
_UintRange63Bit:
	.quad	-9223372036854775808            ; 0x8000000000000000

	.section	__TEXT,__cstring,cstring_literals
l_.str:                                 ; @.str
	.asciz	"%ld ns, avg=%.4f ns/op\n"

	.section	__DWARF,__debug_loc,regular,debug
Lsection_debug_loc:
Ldebug_loc0:
.set Lset0, Lfunc_begin0-Lfunc_begin0
	.quad	Lset0
.set Lset1, Ltmp2-Lfunc_begin0
	.quad	Lset1
	.short	1                               ; Loc expr size
	.byte	80                              ; DW_OP_reg0
.set Lset2, Ltmp3-Lfunc_begin0
	.quad	Lset2
.set Lset3, Lfunc_end0-Lfunc_begin0
	.quad	Lset3
	.short	1                               ; Loc expr size
	.byte	80                              ; DW_OP_reg0
	.quad	0
	.quad	0
Ldebug_loc1:
.set Lset4, Lfunc_begin0-Lfunc_begin0
	.quad	Lset4
.set Lset5, Ltmp1-Lfunc_begin0
	.quad	Lset5
	.short	1                               ; Loc expr size
	.byte	83                              ; DW_OP_reg3
.set Lset6, Ltmp4-Lfunc_begin0
	.quad	Lset6
.set Lset7, Ltmp5-Lfunc_begin0
	.quad	Lset7
	.short	4                               ; Loc expr size
	.byte	163                             ; DW_OP_entry_value
	.byte	1                               ; 1
	.byte	83                              ; DW_OP_reg3
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc2:
.set Lset8, Lfunc_begin0-Lfunc_begin0
	.quad	Lset8
.set Lset9, Ltmp1-Lfunc_begin0
	.quad	Lset9
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	0                               ; 0
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc3:
.set Lset10, Ltmp1-Lfunc_begin0
	.quad	Lset10
.set Lset11, Ltmp3-Lfunc_begin0
	.quad	Lset11
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset12, Ltmp8-Lfunc_begin0
	.quad	Lset12
.set Lset13, Ltmp14-Lfunc_begin0
	.quad	Lset13
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset14, Ltmp16-Lfunc_begin0
	.quad	Lset14
.set Lset15, Ltmp17-Lfunc_begin0
	.quad	Lset15
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset16, Ltmp19-Lfunc_begin0
	.quad	Lset16
.set Lset17, Ltmp20-Lfunc_begin0
	.quad	Lset17
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset18, Ltmp22-Lfunc_begin0
	.quad	Lset18
.set Lset19, Ltmp23-Lfunc_begin0
	.quad	Lset19
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset20, Ltmp25-Lfunc_begin0
	.quad	Lset20
.set Lset21, Ltmp26-Lfunc_begin0
	.quad	Lset21
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset22, Ltmp28-Lfunc_begin0
	.quad	Lset22
.set Lset23, Ltmp29-Lfunc_begin0
	.quad	Lset23
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset24, Ltmp31-Lfunc_begin0
	.quad	Lset24
.set Lset25, Ltmp32-Lfunc_begin0
	.quad	Lset25
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset26, Ltmp34-Lfunc_begin0
	.quad	Lset26
.set Lset27, Ltmp35-Lfunc_begin0
	.quad	Lset27
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
.set Lset28, Ltmp37-Lfunc_begin0
	.quad	Lset28
.set Lset29, Ltmp38-Lfunc_begin0
	.quad	Lset29
	.short	1                               ; Loc expr size
	.byte	93                              ; DW_OP_reg13
	.quad	0
	.quad	0
Ldebug_loc4:
.set Lset30, Ltmp44-Lfunc_begin0
	.quad	Lset30
.set Lset31, Ltmp46-Lfunc_begin0
	.quad	Lset31
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	0                               ; 0
	.byte	159                             ; DW_OP_stack_value
.set Lset32, Ltmp46-Lfunc_begin0
	.quad	Lset32
.set Lset33, Ltmp52-Lfunc_begin0
	.quad	Lset33
	.short	14                              ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	0                               ; 0
	.byte	17                              ; DW_OP_consts
	.byte	208                             ; 80
	.byte	0                               ;
	.byte	131                             ; DW_OP_breg19
	.byte	0                               ; 0
	.byte	34                              ; DW_OP_plus
	.byte	28                              ; DW_OP_minus
	.byte	17                              ; DW_OP_consts
	.byte	168                             ; 168
	.byte	1                               ;
	.byte	27                              ; DW_OP_div
	.byte	159                             ; DW_OP_stack_value
.set Lset34, Ltmp52-Lfunc_begin0
	.quad	Lset34
.set Lset35, Ltmp53-Lfunc_begin0
	.quad	Lset35
	.short	17                              ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	0                               ; 0
	.byte	17                              ; DW_OP_consts
	.byte	208                             ; 80
	.byte	0                               ;
	.byte	131                             ; DW_OP_breg19
	.byte	0                               ; 0
	.byte	34                              ; DW_OP_plus
	.byte	28                              ; DW_OP_minus
	.byte	17                              ; DW_OP_consts
	.byte	168                             ; 168
	.byte	1                               ;
	.byte	27                              ; DW_OP_div
	.byte	17                              ; DW_OP_consts
	.byte	1                               ; 1
	.byte	34                              ; DW_OP_plus
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc5:
.set Lset36, Ltmp44-Lfunc_begin0
	.quad	Lset36
.set Lset37, Ltmp46-Lfunc_begin0
	.quad	Lset37
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	0                               ; 0
	.byte	159                             ; DW_OP_stack_value
.set Lset38, Ltmp46-Lfunc_begin0
	.quad	Lset38
.set Lset39, Ltmp47-Lfunc_begin0
	.quad	Lset39
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	1                               ; 1
	.byte	159                             ; DW_OP_stack_value
.set Lset40, Ltmp47-Lfunc_begin0
	.quad	Lset40
.set Lset41, Ltmp48-Lfunc_begin0
	.quad	Lset41
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	5                               ; 5
	.byte	159                             ; DW_OP_stack_value
.set Lset42, Ltmp48-Lfunc_begin0
	.quad	Lset42
.set Lset43, Ltmp49-Lfunc_begin0
	.quad	Lset43
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	9                               ; 9
	.byte	159                             ; DW_OP_stack_value
.set Lset44, Ltmp49-Lfunc_begin0
	.quad	Lset44
.set Lset45, Ltmp50-Lfunc_begin0
	.quad	Lset45
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	13                              ; 13
	.byte	159                             ; DW_OP_stack_value
.set Lset46, Ltmp50-Lfunc_begin0
	.quad	Lset46
.set Lset47, Ltmp51-Lfunc_begin0
	.quad	Lset47
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	17                              ; 17
	.byte	159                             ; DW_OP_stack_value
.set Lset48, Ltmp51-Lfunc_begin0
	.quad	Lset48
.set Lset49, Ltmp52-Lfunc_begin0
	.quad	Lset49
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	20                              ; 20
	.byte	159                             ; DW_OP_stack_value
.set Lset50, Ltmp52-Lfunc_begin0
	.quad	Lset50
.set Lset51, Ltmp53-Lfunc_begin0
	.quad	Lset51
	.short	3                               ; Loc expr size
	.byte	120                             ; DW_OP_breg8
	.byte	0                               ; 0
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc6:
.set Lset52, Ltmp44-Lfunc_begin0
	.quad	Lset52
.set Lset53, Ltmp68-Lfunc_begin0
	.quad	Lset53
	.short	1                               ; Loc expr size
	.byte	99                              ; DW_OP_reg19
	.quad	0
	.quad	0
Ldebug_loc7:
.set Lset54, Ltmp46-Lfunc_begin0
	.quad	Lset54
.set Lset55, Ltmp47-Lfunc_begin0
	.quad	Lset55
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	1                               ; 1
	.byte	159                             ; DW_OP_stack_value
.set Lset56, Ltmp47-Lfunc_begin0
	.quad	Lset56
.set Lset57, Ltmp48-Lfunc_begin0
	.quad	Lset57
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	5                               ; 5
	.byte	159                             ; DW_OP_stack_value
.set Lset58, Ltmp48-Lfunc_begin0
	.quad	Lset58
.set Lset59, Ltmp49-Lfunc_begin0
	.quad	Lset59
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	9                               ; 9
	.byte	159                             ; DW_OP_stack_value
.set Lset60, Ltmp49-Lfunc_begin0
	.quad	Lset60
.set Lset61, Ltmp50-Lfunc_begin0
	.quad	Lset61
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	13                              ; 13
	.byte	159                             ; DW_OP_stack_value
.set Lset62, Ltmp50-Lfunc_begin0
	.quad	Lset62
.set Lset63, Ltmp51-Lfunc_begin0
	.quad	Lset63
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	17                              ; 17
	.byte	159                             ; DW_OP_stack_value
.set Lset64, Ltmp51-Lfunc_begin0
	.quad	Lset64
.set Lset65, Ltmp52-Lfunc_begin0
	.quad	Lset65
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	20                              ; 20
	.byte	159                             ; DW_OP_stack_value
.set Lset66, Ltmp52-Lfunc_begin0
	.quad	Lset66
.set Lset67, Ltmp55-Lfunc_begin0
	.quad	Lset67
	.short	3                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	21                              ; 21
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc8:
.set Lset68, Ltmp56-Lfunc_begin0
	.quad	Lset68
.set Lset69, Lfunc_end1-Lfunc_begin0
	.quad	Lset69
	.short	5                               ; Loc expr size
	.byte	17                              ; DW_OP_consts
	.byte	136                             ; 21000
	.byte	164                             ;
	.byte	1                               ;
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc9:
.set Lset70, Ltmp56-Lfunc_begin0
	.quad	Lset70
.set Lset71, Lfunc_end1-Lfunc_begin0
	.quad	Lset71
	.short	5                               ; Loc expr size
	.byte	16                              ; DW_OP_constu
	.byte	192                             ; 1000000
	.byte	132                             ;
	.byte	61                              ;
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc10:
.set Lset72, Ltmp58-Lfunc_begin0
	.quad	Lset72
.set Lset73, Ltmp61-Lfunc_begin0
	.quad	Lset73
	.short	2                               ; Loc expr size
	.byte	48                              ; DW_OP_lit0
	.byte	159                             ; DW_OP_stack_value
.set Lset74, Ltmp61-Lfunc_begin0
	.quad	Lset74
.set Lset75, Ltmp69-Lfunc_begin0
	.quad	Lset75
	.short	2                               ; Loc expr size
	.byte	143                             ; DW_OP_breg31
	.byte	24                              ; 24
	.quad	0
	.quad	0
Ldebug_loc11:
.set Lset76, Ltmp58-Lfunc_begin0
	.quad	Lset76
.set Lset77, Ltmp68-Lfunc_begin0
	.quad	Lset77
	.short	1                               ; Loc expr size
	.byte	100                             ; DW_OP_reg20
	.quad	0
	.quad	0
Ldebug_loc12:
.set Lset78, Ltmp60-Lfunc_begin0
	.quad	Lset78
.set Lset79, Ltmp62-Lfunc_begin0
	.quad	Lset79
	.short	11                              ; Loc expr size
	.byte	134                             ; DW_OP_breg22
	.byte	0                               ; 0
	.byte	17                              ; DW_OP_consts
	.byte	192                             ; 1000000
	.byte	132                             ;
	.byte	61                              ;
	.byte	28                              ; DW_OP_minus
	.byte	17                              ; DW_OP_consts
	.byte	127                             ; -1
	.byte	27                              ; DW_OP_div
	.byte	159                             ; DW_OP_stack_value
.set Lset80, Ltmp62-Lfunc_begin0
	.quad	Lset80
.set Lset81, Ltmp63-Lfunc_begin0
	.quad	Lset81
	.short	14                              ; Loc expr size
	.byte	134                             ; DW_OP_breg22
	.byte	0                               ; 0
	.byte	17                              ; DW_OP_consts
	.byte	192                             ; 1000000
	.byte	132                             ;
	.byte	61                              ;
	.byte	28                              ; DW_OP_minus
	.byte	17                              ; DW_OP_consts
	.byte	127                             ; -1
	.byte	27                              ; DW_OP_div
	.byte	17                              ; DW_OP_consts
	.byte	1                               ; 1
	.byte	34                              ; DW_OP_plus
	.byte	159                             ; DW_OP_stack_value
	.quad	0
	.quad	0
Ldebug_loc13:
.set Lset82, Ltmp66-Lfunc_begin0
	.quad	Lset82
.set Lset83, Ltmp67-Lfunc_begin0
	.quad	Lset83
	.short	1                               ; Loc expr size
	.byte	88                              ; DW_OP_reg8
	.quad	0
	.quad	0
	.section	__DWARF,__debug_abbrev,regular,debug
Lsection_abbrev:
	.byte	1                               ; Abbreviation Code
	.byte	17                              ; DW_TAG_compile_unit
	.byte	1                               ; DW_CHILDREN_yes
	.byte	37                              ; DW_AT_producer
	.byte	14                              ; DW_FORM_strp
	.byte	19                              ; DW_AT_language
	.byte	5                               ; DW_FORM_data2
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.ascii	"\202|"                         ; DW_AT_LLVM_sysroot
	.byte	14                              ; DW_FORM_strp
	.ascii	"\357\177"                      ; DW_AT_APPLE_sdk
	.byte	14                              ; DW_FORM_strp
	.byte	16                              ; DW_AT_stmt_list
	.byte	23                              ; DW_FORM_sec_offset
	.byte	27                              ; DW_AT_comp_dir
	.byte	14                              ; DW_FORM_strp
	.ascii	"\341\177"                      ; DW_AT_APPLE_optimized
	.byte	25                              ; DW_FORM_flag_present
	.byte	17                              ; DW_AT_low_pc
	.byte	1                               ; DW_FORM_addr
	.byte	18                              ; DW_AT_high_pc
	.byte	6                               ; DW_FORM_data4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	2                               ; Abbreviation Code
	.byte	52                              ; DW_TAG_variable
	.byte	0                               ; DW_CHILDREN_no
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	63                              ; DW_AT_external
	.byte	25                              ; DW_FORM_flag_present
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	2                               ; DW_AT_location
	.byte	24                              ; DW_FORM_exprloc
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	3                               ; Abbreviation Code
	.byte	38                              ; DW_TAG_const_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	4                               ; Abbreviation Code
	.byte	22                              ; DW_TAG_typedef
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	5                               ; Abbreviation Code
	.byte	36                              ; DW_TAG_base_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	62                              ; DW_AT_encoding
	.byte	11                              ; DW_FORM_data1
	.byte	11                              ; DW_AT_byte_size
	.byte	11                              ; DW_FORM_data1
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	6                               ; Abbreviation Code
	.byte	52                              ; DW_TAG_variable
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	2                               ; DW_AT_location
	.byte	24                              ; DW_FORM_exprloc
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	7                               ; Abbreviation Code
	.byte	1                               ; DW_TAG_array_type
	.byte	1                               ; DW_CHILDREN_yes
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	8                               ; Abbreviation Code
	.byte	33                              ; DW_TAG_subrange_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	55                              ; DW_AT_count
	.byte	11                              ; DW_FORM_data1
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	9                               ; Abbreviation Code
	.byte	36                              ; DW_TAG_base_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	11                              ; DW_AT_byte_size
	.byte	11                              ; DW_FORM_data1
	.byte	62                              ; DW_AT_encoding
	.byte	11                              ; DW_FORM_data1
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	10                              ; Abbreviation Code
	.byte	15                              ; DW_TAG_pointer_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	11                              ; Abbreviation Code
	.byte	15                              ; DW_TAG_pointer_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	12                              ; Abbreviation Code
	.byte	46                              ; DW_TAG_subprogram
	.byte	1                               ; DW_CHILDREN_yes
	.byte	17                              ; DW_AT_low_pc
	.byte	1                               ; DW_FORM_addr
	.byte	18                              ; DW_AT_high_pc
	.byte	6                               ; DW_FORM_data4
	.ascii	"\347\177"                      ; DW_AT_APPLE_omit_frame_ptr
	.byte	25                              ; DW_FORM_flag_present
	.byte	64                              ; DW_AT_frame_base
	.byte	24                              ; DW_FORM_exprloc
	.byte	122                             ; DW_AT_call_all_calls
	.byte	25                              ; DW_FORM_flag_present
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	39                              ; DW_AT_prototyped
	.byte	25                              ; DW_FORM_flag_present
	.byte	63                              ; DW_AT_external
	.byte	25                              ; DW_FORM_flag_present
	.ascii	"\341\177"                      ; DW_AT_APPLE_optimized
	.byte	25                              ; DW_FORM_flag_present
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	13                              ; Abbreviation Code
	.byte	5                               ; DW_TAG_formal_parameter
	.byte	0                               ; DW_CHILDREN_no
	.byte	2                               ; DW_AT_location
	.byte	23                              ; DW_FORM_sec_offset
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	14                              ; Abbreviation Code
	.byte	5                               ; DW_TAG_formal_parameter
	.byte	0                               ; DW_CHILDREN_no
	.byte	2                               ; DW_AT_location
	.byte	24                              ; DW_FORM_exprloc
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	15                              ; Abbreviation Code
	.byte	11                              ; DW_TAG_lexical_block
	.byte	1                               ; DW_CHILDREN_yes
	.byte	17                              ; DW_AT_low_pc
	.byte	1                               ; DW_FORM_addr
	.byte	18                              ; DW_AT_high_pc
	.byte	6                               ; DW_FORM_data4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	16                              ; Abbreviation Code
	.byte	52                              ; DW_TAG_variable
	.byte	0                               ; DW_CHILDREN_no
	.byte	2                               ; DW_AT_location
	.byte	23                              ; DW_FORM_sec_offset
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	17                              ; Abbreviation Code
	.byte	11                              ; DW_TAG_lexical_block
	.byte	1                               ; DW_CHILDREN_yes
	.byte	85                              ; DW_AT_ranges
	.byte	23                              ; DW_FORM_sec_offset
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	18                              ; Abbreviation Code
	.byte	46                              ; DW_TAG_subprogram
	.byte	1                               ; DW_CHILDREN_yes
	.byte	17                              ; DW_AT_low_pc
	.byte	1                               ; DW_FORM_addr
	.byte	18                              ; DW_AT_high_pc
	.byte	6                               ; DW_FORM_data4
	.byte	64                              ; DW_AT_frame_base
	.byte	24                              ; DW_FORM_exprloc
	.byte	122                             ; DW_AT_call_all_calls
	.byte	25                              ; DW_FORM_flag_present
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	63                              ; DW_AT_external
	.byte	25                              ; DW_FORM_flag_present
	.ascii	"\341\177"                      ; DW_AT_APPLE_optimized
	.byte	25                              ; DW_FORM_flag_present
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	19                              ; Abbreviation Code
	.byte	52                              ; DW_TAG_variable
	.byte	0                               ; DW_CHILDREN_no
	.byte	2                               ; DW_AT_location
	.byte	24                              ; DW_FORM_exprloc
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	20                              ; Abbreviation Code
	.byte	52                              ; DW_TAG_variable
	.byte	0                               ; DW_CHILDREN_no
	.byte	28                              ; DW_AT_const_value
	.byte	13                              ; DW_FORM_sdata
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	21                              ; Abbreviation Code
	.byte	52                              ; DW_TAG_variable
	.byte	0                               ; DW_CHILDREN_no
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	22                              ; Abbreviation Code
	.byte	72                              ; DW_TAG_call_site
	.byte	1                               ; DW_CHILDREN_yes
	.byte	127                             ; DW_AT_call_origin
	.byte	19                              ; DW_FORM_ref4
	.byte	125                             ; DW_AT_call_return_pc
	.byte	1                               ; DW_FORM_addr
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	23                              ; Abbreviation Code
	.byte	73                              ; DW_TAG_call_site_parameter
	.byte	0                               ; DW_CHILDREN_no
	.byte	2                               ; DW_AT_location
	.byte	24                              ; DW_FORM_exprloc
	.byte	126                             ; DW_AT_call_value
	.byte	24                              ; DW_FORM_exprloc
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	24                              ; Abbreviation Code
	.byte	46                              ; DW_TAG_subprogram
	.byte	1                               ; DW_CHILDREN_yes
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	39                              ; DW_AT_prototyped
	.byte	25                              ; DW_FORM_flag_present
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	60                              ; DW_AT_declaration
	.byte	25                              ; DW_FORM_flag_present
	.byte	63                              ; DW_AT_external
	.byte	25                              ; DW_FORM_flag_present
	.ascii	"\341\177"                      ; DW_AT_APPLE_optimized
	.byte	25                              ; DW_FORM_flag_present
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	25                              ; Abbreviation Code
	.byte	5                               ; DW_TAG_formal_parameter
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	26                              ; Abbreviation Code
	.byte	55                              ; DW_TAG_restrict_type
	.byte	0                               ; DW_CHILDREN_no
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	27                              ; Abbreviation Code
	.byte	19                              ; DW_TAG_structure_type
	.byte	1                               ; DW_CHILDREN_yes
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	11                              ; DW_AT_byte_size
	.byte	11                              ; DW_FORM_data1
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	28                              ; Abbreviation Code
	.byte	13                              ; DW_TAG_member
	.byte	0                               ; DW_CHILDREN_no
	.byte	3                               ; DW_AT_name
	.byte	14                              ; DW_FORM_strp
	.byte	73                              ; DW_AT_type
	.byte	19                              ; DW_FORM_ref4
	.byte	58                              ; DW_AT_decl_file
	.byte	11                              ; DW_FORM_data1
	.byte	59                              ; DW_AT_decl_line
	.byte	11                              ; DW_FORM_data1
	.byte	56                              ; DW_AT_data_member_location
	.byte	11                              ; DW_FORM_data1
	.byte	0                               ; EOM(1)
	.byte	0                               ; EOM(2)
	.byte	0                               ; EOM(3)
	.section	__DWARF,__debug_info,regular,debug
Lsection_info:
Lcu_begin0:
.set Lset84, Ldebug_info_end0-Ldebug_info_start0 ; Length of Unit
	.long	Lset84
Ldebug_info_start0:
	.short	4                               ; DWARF version number
.set Lset85, Lsection_abbrev-Lsection_abbrev ; Offset Into Abbrev. Section
	.long	Lset85
	.byte	8                               ; Address Size (in bytes)
	.byte	1                               ; Abbrev [1] 0xb:0x3f0 DW_TAG_compile_unit
	.long	0                               ; DW_AT_producer
	.short	12                              ; DW_AT_language
	.long	48                              ; DW_AT_name
	.long	69                              ; DW_AT_LLVM_sysroot
	.long	121                             ; DW_AT_APPLE_sdk
.set Lset86, Lline_table_start0-Lsection_line ; DW_AT_stmt_list
	.long	Lset86
	.long	132                             ; DW_AT_comp_dir
                                        ; DW_AT_APPLE_optimized
	.quad	Lfunc_begin0                    ; DW_AT_low_pc
.set Lset87, Lfunc_end1-Lfunc_begin0    ; DW_AT_high_pc
	.long	Lset87
	.byte	2                               ; Abbrev [2] 0x32:0x15 DW_TAG_variable
	.long	224                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	115                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange7Bit
	.byte	3                               ; Abbrev [3] 0x47:0x5 DW_TAG_const_type
	.long	76                              ; DW_AT_type
	.byte	4                               ; Abbrev [4] 0x4c:0xb DW_TAG_typedef
	.long	87                              ; DW_AT_type
	.long	238                             ; DW_AT_name
	.byte	1                               ; DW_AT_decl_file
	.byte	31                              ; DW_AT_decl_line
	.byte	5                               ; Abbrev [5] 0x57:0x7 DW_TAG_base_type
	.long	247                             ; DW_AT_name
	.byte	7                               ; DW_AT_encoding
	.byte	8                               ; DW_AT_byte_size
	.byte	2                               ; Abbrev [2] 0x5e:0x15 DW_TAG_variable
	.long	266                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	116                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange14Bit
	.byte	2                               ; Abbrev [2] 0x73:0x15 DW_TAG_variable
	.long	281                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	117                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange21Bit
	.byte	2                               ; Abbrev [2] 0x88:0x15 DW_TAG_variable
	.long	296                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	118                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange28Bit
	.byte	2                               ; Abbrev [2] 0x9d:0x15 DW_TAG_variable
	.long	311                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	119                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange35Bit
	.byte	2                               ; Abbrev [2] 0xb2:0x15 DW_TAG_variable
	.long	326                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	120                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange42Bit
	.byte	2                               ; Abbrev [2] 0xc7:0x15 DW_TAG_variable
	.long	341                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	121                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange49Bit
	.byte	2                               ; Abbrev [2] 0xdc:0x15 DW_TAG_variable
	.long	356                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	122                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange56Bit
	.byte	2                               ; Abbrev [2] 0xf1:0x15 DW_TAG_variable
	.long	371                             ; DW_AT_name
	.long	71                              ; DW_AT_type
                                        ; DW_AT_external
	.byte	2                               ; DW_AT_decl_file
	.byte	123                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	_UintRange63Bit
	.byte	6                               ; Abbrev [6] 0x106:0x11 DW_TAG_variable
	.long	279                             ; DW_AT_type
	.byte	2                               ; DW_AT_decl_file
	.byte	163                             ; DW_AT_decl_line
	.byte	9                               ; DW_AT_location
	.byte	3
	.quad	l_.str
	.byte	7                               ; Abbrev [7] 0x117:0xc DW_TAG_array_type
	.long	291                             ; DW_AT_type
	.byte	8                               ; Abbrev [8] 0x11c:0x6 DW_TAG_subrange_type
	.long	298                             ; DW_AT_type
	.byte	24                              ; DW_AT_count
	.byte	0                               ; End Of Children Mark
	.byte	5                               ; Abbrev [5] 0x123:0x7 DW_TAG_base_type
	.long	386                             ; DW_AT_name
	.byte	6                               ; DW_AT_encoding
	.byte	1                               ; DW_AT_byte_size
	.byte	9                               ; Abbrev [9] 0x12a:0x7 DW_TAG_base_type
	.long	391                             ; DW_AT_name
	.byte	8                               ; DW_AT_byte_size
	.byte	7                               ; DW_AT_encoding
	.byte	4                               ; Abbrev [4] 0x131:0xb DW_TAG_typedef
	.long	316                             ; DW_AT_type
	.long	412                             ; DW_AT_name
	.byte	3                               ; DW_AT_decl_file
	.byte	31                              ; DW_AT_decl_line
	.byte	5                               ; Abbrev [5] 0x13c:0x7 DW_TAG_base_type
	.long	420                             ; DW_AT_name
	.byte	8                               ; DW_AT_encoding
	.byte	1                               ; DW_AT_byte_size
	.byte	10                              ; Abbrev [10] 0x143:0x5 DW_TAG_pointer_type
	.long	76                              ; DW_AT_type
	.byte	4                               ; Abbrev [4] 0x148:0xb DW_TAG_typedef
	.long	339                             ; DW_AT_type
	.long	434                             ; DW_AT_name
	.byte	4                               ; DW_AT_decl_file
	.byte	30                              ; DW_AT_decl_line
	.byte	5                               ; Abbrev [5] 0x153:0x7 DW_TAG_base_type
	.long	442                             ; DW_AT_name
	.byte	5                               ; DW_AT_encoding
	.byte	8                               ; DW_AT_byte_size
	.byte	11                              ; Abbrev [11] 0x15a:0x1 DW_TAG_pointer_type
	.byte	5                               ; Abbrev [5] 0x15b:0x7 DW_TAG_base_type
	.long	452                             ; DW_AT_name
	.byte	4                               ; DW_AT_encoding
	.byte	8                               ; DW_AT_byte_size
	.byte	12                              ; Abbrev [12] 0x162:0x8d DW_TAG_subprogram
	.quad	Lfunc_begin0                    ; DW_AT_low_pc
.set Lset88, Lfunc_end0-Lfunc_begin0    ; DW_AT_high_pc
	.long	Lset88
                                        ; DW_AT_APPLE_omit_frame_ptr
	.byte	1                               ; DW_AT_frame_base
	.byte	111
                                        ; DW_AT_call_all_calls
	.long	459                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	6                               ; DW_AT_decl_line
                                        ; DW_AT_prototyped
                                        ; DW_AT_external
                                        ; DW_AT_APPLE_optimized
	.byte	13                              ; Abbrev [13] 0x177:0xf DW_TAG_formal_parameter
.set Lset89, Ldebug_loc0-Lsection_debug_loc ; DW_AT_location
	.long	Lset89
	.long	573                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	6                               ; DW_AT_decl_line
	.long	991                             ; DW_AT_type
	.byte	14                              ; Abbrev [14] 0x186:0xd DW_TAG_formal_parameter
	.byte	1                               ; DW_AT_location
	.byte	81
	.long	577                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	6                               ; DW_AT_decl_line
	.long	323                             ; DW_AT_type
	.byte	14                              ; Abbrev [14] 0x193:0xd DW_TAG_formal_parameter
	.byte	1                               ; DW_AT_location
	.byte	82
	.long	584                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	6                               ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	13                              ; Abbrev [13] 0x1a0:0xf DW_TAG_formal_parameter
.set Lset90, Ldebug_loc1-Lsection_debug_loc ; DW_AT_location
	.long	Lset90
	.long	597                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	6                               ; DW_AT_decl_line
	.long	996                             ; DW_AT_type
	.byte	14                              ; Abbrev [14] 0x1af:0xd DW_TAG_formal_parameter
	.byte	1                               ; DW_AT_location
	.byte	84
	.long	601                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	6                               ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	15                              ; Abbrev [15] 0x1bc:0x32 DW_TAG_lexical_block
	.quad	Lfunc_begin0                    ; DW_AT_low_pc
.set Lset91, Ltmp40-Lfunc_begin0        ; DW_AT_high_pc
	.long	Lset91
	.byte	16                              ; Abbrev [16] 0x1c9:0xf DW_TAG_variable
.set Lset92, Ldebug_loc2-Lsection_debug_loc ; DW_AT_location
	.long	Lset92
	.long	607                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	7                               ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	17                              ; Abbrev [17] 0x1d8:0x15 DW_TAG_lexical_block
.set Lset93, Ldebug_ranges0-Ldebug_range ; DW_AT_ranges
	.long	Lset93
	.byte	16                              ; Abbrev [16] 0x1dd:0xf DW_TAG_variable
.set Lset94, Ldebug_loc3-Lsection_debug_loc ; DW_AT_location
	.long	Lset94
	.long	609                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	8                               ; DW_AT_decl_line
	.long	76                              ; DW_AT_type
	.byte	0                               ; End Of Children Mark
	.byte	0                               ; End Of Children Mark
	.byte	0                               ; End Of Children Mark
	.byte	18                              ; Abbrev [18] 0x1ef:0x17b DW_TAG_subprogram
	.quad	Lfunc_begin1                    ; DW_AT_low_pc
.set Lset95, Lfunc_end1-Lfunc_begin1    ; DW_AT_high_pc
	.long	Lset95
	.byte	1                               ; DW_AT_frame_base
	.byte	109
                                        ; DW_AT_call_all_calls
	.long	476                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	126                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
                                        ; DW_AT_external
                                        ; DW_AT_APPLE_optimized
	.byte	19                              ; Abbrev [19] 0x208:0xe DW_TAG_variable
	.byte	2                               ; DW_AT_location
	.byte	143
	.byte	48
	.long	613                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	152                             ; DW_AT_decl_line
	.long	913                             ; DW_AT_type
	.byte	19                              ; Abbrev [19] 0x216:0xe DW_TAG_variable
	.byte	2                               ; DW_AT_location
	.byte	143
	.byte	32
	.long	619                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	152                             ; DW_AT_decl_line
	.long	913                             ; DW_AT_type
	.byte	20                              ; Abbrev [20] 0x224:0xd DW_TAG_variable
	.asciz	"\344"                          ; DW_AT_const_value
	.long	623                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	140                             ; DW_AT_decl_line
	.long	1001                            ; DW_AT_type
	.byte	20                              ; Abbrev [20] 0x231:0xc DW_TAG_variable
	.byte	21                              ; DW_AT_const_value
	.long	601                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	141                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x23d:0xf DW_TAG_variable
.set Lset96, Ldebug_loc5-Lsection_debug_loc ; DW_AT_location
	.long	Lset96
	.long	635                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	143                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x24c:0xf DW_TAG_variable
.set Lset97, Ldebug_loc6-Lsection_debug_loc ; DW_AT_location
	.long	Lset97
	.long	639                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	142                             ; DW_AT_decl_line
	.long	996                             ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x25b:0xf DW_TAG_variable
.set Lset98, Ldebug_loc8-Lsection_debug_loc ; DW_AT_location
	.long	Lset98
	.long	645                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	153                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x26a:0xf DW_TAG_variable
.set Lset99, Ldebug_loc9-Lsection_debug_loc ; DW_AT_location
	.long	Lset99
	.long	652                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	151                             ; DW_AT_decl_line
	.long	1001                            ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x279:0xf DW_TAG_variable
.set Lset100, Ldebug_loc10-Lsection_debug_loc ; DW_AT_location
	.long	Lset100
	.long	661                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	155                             ; DW_AT_decl_line
	.long	76                              ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x288:0xf DW_TAG_variable
.set Lset101, Ldebug_loc11-Lsection_debug_loc ; DW_AT_location
	.long	Lset101
	.long	573                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	154                             ; DW_AT_decl_line
	.long	991                             ; DW_AT_type
	.byte	16                              ; Abbrev [16] 0x297:0xf DW_TAG_variable
.set Lset102, Ldebug_loc13-Lsection_debug_loc ; DW_AT_location
	.long	Lset102
	.long	668                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	162                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	21                              ; Abbrev [21] 0x2a6:0xb DW_TAG_variable
	.long	674                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	127                             ; DW_AT_decl_line
	.long	1006                            ; DW_AT_type
	.byte	15                              ; Abbrev [15] 0x2b1:0x3a DW_TAG_lexical_block
	.quad	Ltmp45                          ; DW_AT_low_pc
.set Lset103, Ltmp55-Ltmp45             ; DW_AT_high_pc
	.long	Lset103
	.byte	16                              ; Abbrev [16] 0x2be:0xf DW_TAG_variable
.set Lset104, Ldebug_loc4-Lsection_debug_loc ; DW_AT_location
	.long	Lset104
	.long	607                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	144                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	15                              ; Abbrev [15] 0x2cd:0x1d DW_TAG_lexical_block
	.quad	Ltmp46                          ; DW_AT_low_pc
.set Lset105, Ltmp52-Ltmp46             ; DW_AT_high_pc
	.long	Lset105
	.byte	16                              ; Abbrev [16] 0x2da:0xf DW_TAG_variable
.set Lset106, Ldebug_loc7-Lsection_debug_loc ; DW_AT_location
	.long	Lset106
	.long	643                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	145                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	0                               ; End Of Children Mark
	.byte	0                               ; End Of Children Mark
	.byte	15                              ; Abbrev [15] 0x2eb:0x1d DW_TAG_lexical_block
	.quad	Ltmp60                          ; DW_AT_low_pc
.set Lset107, Ltmp64-Ltmp60             ; DW_AT_high_pc
	.long	Lset107
	.byte	16                              ; Abbrev [16] 0x2f8:0xf DW_TAG_variable
.set Lset108, Ldebug_loc12-Lsection_debug_loc ; DW_AT_location
	.long	Lset108
	.long	607                             ; DW_AT_name
	.byte	2                               ; DW_AT_decl_file
	.byte	157                             ; DW_AT_decl_line
	.long	896                             ; DW_AT_type
	.byte	0                               ; End Of Children Mark
	.byte	22                              ; Abbrev [22] 0x308:0x19 DW_TAG_call_site
	.long	874                             ; DW_AT_call_origin
	.quad	Ltmp59                          ; DW_AT_call_return_pc
	.byte	23                              ; Abbrev [23] 0x315:0x5 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	81
	.byte	1                               ; DW_AT_call_value
	.byte	48
	.byte	23                              ; Abbrev [23] 0x31a:0x6 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	80
	.byte	2                               ; DW_AT_call_value
	.byte	143
	.byte	48
	.byte	0                               ; End Of Children Mark
	.byte	22                              ; Abbrev [22] 0x321:0x2f DW_TAG_call_site
	.long	354                             ; DW_AT_call_origin
	.quad	Ltmp62                          ; DW_AT_call_return_pc
	.byte	23                              ; Abbrev [23] 0x32e:0x7 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	84
	.byte	3                               ; DW_AT_call_value
	.byte	16
	.ascii	"\264\020"
	.byte	23                              ; Abbrev [23] 0x335:0x6 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	83
	.byte	2                               ; DW_AT_call_value
	.byte	131
	.byte	0
	.byte	23                              ; Abbrev [23] 0x33b:0x8 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	82
	.byte	4                               ; DW_AT_call_value
	.byte	16
	.ascii	"\210\244\001"
	.byte	23                              ; Abbrev [23] 0x343:0x6 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	80
	.byte	2                               ; DW_AT_call_value
	.byte	132
	.byte	0
	.byte	23                              ; Abbrev [23] 0x349:0x6 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	81
	.byte	2                               ; DW_AT_call_value
	.byte	143
	.byte	24
	.byte	0                               ; End Of Children Mark
	.byte	22                              ; Abbrev [22] 0x350:0x19 DW_TAG_call_site
	.long	874                             ; DW_AT_call_origin
	.quad	Ltmp65                          ; DW_AT_call_return_pc
	.byte	23                              ; Abbrev [23] 0x35d:0x5 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	81
	.byte	1                               ; DW_AT_call_value
	.byte	48
	.byte	23                              ; Abbrev [23] 0x362:0x6 DW_TAG_call_site_parameter
	.byte	1                               ; DW_AT_location
	.byte	80
	.byte	2                               ; DW_AT_call_value
	.byte	143
	.byte	32
	.byte	0                               ; End Of Children Mark
	.byte	0                               ; End Of Children Mark
	.byte	24                              ; Abbrev [24] 0x36a:0x16 DW_TAG_subprogram
	.long	481                             ; DW_AT_name
	.byte	5                               ; DW_AT_decl_file
	.byte	198                             ; DW_AT_decl_line
                                        ; DW_AT_prototyped
	.long	896                             ; DW_AT_type
                                        ; DW_AT_declaration
                                        ; DW_AT_external
                                        ; DW_AT_APPLE_optimized
	.byte	25                              ; Abbrev [25] 0x375:0x5 DW_TAG_formal_parameter
	.long	903                             ; DW_AT_type
	.byte	25                              ; Abbrev [25] 0x37a:0x5 DW_TAG_formal_parameter
	.long	986                             ; DW_AT_type
	.byte	0                               ; End Of Children Mark
	.byte	5                               ; Abbrev [5] 0x380:0x7 DW_TAG_base_type
	.long	494                             ; DW_AT_name
	.byte	5                               ; DW_AT_encoding
	.byte	4                               ; DW_AT_byte_size
	.byte	26                              ; Abbrev [26] 0x387:0x5 DW_TAG_restrict_type
	.long	908                             ; DW_AT_type
	.byte	10                              ; Abbrev [10] 0x38c:0x5 DW_TAG_pointer_type
	.long	913                             ; DW_AT_type
	.byte	27                              ; Abbrev [27] 0x391:0x21 DW_TAG_structure_type
	.long	498                             ; DW_AT_name
	.byte	16                              ; DW_AT_byte_size
	.byte	7                               ; DW_AT_decl_file
	.byte	34                              ; DW_AT_decl_line
	.byte	28                              ; Abbrev [28] 0x399:0xc DW_TAG_member
	.long	506                             ; DW_AT_name
	.long	946                             ; DW_AT_type
	.byte	7                               ; DW_AT_decl_file
	.byte	36                              ; DW_AT_decl_line
	.byte	0                               ; DW_AT_data_member_location
	.byte	28                              ; Abbrev [28] 0x3a5:0xc DW_TAG_member
	.long	534                             ; DW_AT_name
	.long	964                             ; DW_AT_type
	.byte	7                               ; DW_AT_decl_file
	.byte	37                              ; DW_AT_decl_line
	.byte	8                               ; DW_AT_data_member_location
	.byte	0                               ; End Of Children Mark
	.byte	4                               ; Abbrev [4] 0x3b2:0xb DW_TAG_typedef
	.long	957                             ; DW_AT_type
	.long	513                             ; DW_AT_name
	.byte	6                               ; DW_AT_decl_file
	.byte	98                              ; DW_AT_decl_line
	.byte	5                               ; Abbrev [5] 0x3bd:0x7 DW_TAG_base_type
	.long	529                             ; DW_AT_name
	.byte	5                               ; DW_AT_encoding
	.byte	8                               ; DW_AT_byte_size
	.byte	4                               ; Abbrev [4] 0x3c4:0xb DW_TAG_typedef
	.long	975                             ; DW_AT_type
	.long	542                             ; DW_AT_name
	.byte	8                               ; DW_AT_decl_file
	.byte	74                              ; DW_AT_decl_line
	.byte	4                               ; Abbrev [4] 0x3cf:0xb DW_TAG_typedef
	.long	896                             ; DW_AT_type
	.long	563                             ; DW_AT_name
	.byte	6                               ; DW_AT_decl_file
	.byte	22                              ; DW_AT_decl_line
	.byte	26                              ; Abbrev [26] 0x3da:0x5 DW_TAG_restrict_type
	.long	346                             ; DW_AT_type
	.byte	10                              ; Abbrev [10] 0x3df:0x5 DW_TAG_pointer_type
	.long	305                             ; DW_AT_type
	.byte	10                              ; Abbrev [10] 0x3e4:0x5 DW_TAG_pointer_type
	.long	328                             ; DW_AT_type
	.byte	3                               ; Abbrev [3] 0x3e9:0x5 DW_TAG_const_type
	.long	896                             ; DW_AT_type
	.byte	7                               ; Abbrev [7] 0x3ee:0xc DW_TAG_array_type
	.long	76                              ; DW_AT_type
	.byte	8                               ; Abbrev [8] 0x3f3:0x6 DW_TAG_subrange_type
	.long	298                             ; DW_AT_type
	.byte	21                              ; DW_AT_count
	.byte	0                               ; End Of Children Mark
	.byte	0                               ; End Of Children Mark
Ldebug_info_end0:
	.section	__DWARF,__debug_ranges,regular,debug
Ldebug_range:
Ldebug_ranges0:
.set Lset109, Ltmp1-Lfunc_begin0
	.quad	Lset109
.set Lset110, Ltmp3-Lfunc_begin0
	.quad	Lset110
.set Lset111, Ltmp7-Lfunc_begin0
	.quad	Lset111
.set Lset112, Ltmp40-Lfunc_begin0
	.quad	Lset112
	.quad	0
	.quad	0
	.section	__DWARF,__debug_str,regular,debug
Linfo_string:
	.asciz	"Apple clang version 15.0.0 (clang-1500.1.0.2.5)" ; string offset=0
	.asciz	"marshal_var_int64s.c"          ; string offset=48
	.asciz	"/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk" ; string offset=69
	.asciz	"MacOSX.sdk"                    ; string offset=121
	.asciz	"/Users/fuchunzhang/code/github.com/ahfuzhang/VictoriaMetrics_cluster_v1.96.0/lib/encoding/c" ; string offset=132
	.asciz	"UintRange7Bit"                 ; string offset=224
	.asciz	"uint64_t"                      ; string offset=238
	.asciz	"unsigned long long"            ; string offset=247
	.asciz	"UintRange14Bit"                ; string offset=266
	.asciz	"UintRange21Bit"                ; string offset=281
	.asciz	"UintRange28Bit"                ; string offset=296
	.asciz	"UintRange35Bit"                ; string offset=311
	.asciz	"UintRange42Bit"                ; string offset=326
	.asciz	"UintRange49Bit"                ; string offset=341
	.asciz	"UintRange56Bit"                ; string offset=356
	.asciz	"UintRange63Bit"                ; string offset=371
	.asciz	"char"                          ; string offset=386
	.asciz	"__ARRAY_SIZE_TYPE__"           ; string offset=391
	.byte	0                               ; string offset=411
	.asciz	"uint8_t"                       ; string offset=412
	.asciz	"unsigned char"                 ; string offset=420
	.asciz	"int64_t"                       ; string offset=434
	.asciz	"long long"                     ; string offset=442
	.asciz	"double"                        ; string offset=452
	.asciz	"MarshalVarInt64s"              ; string offset=459
	.asciz	"main"                          ; string offset=476
	.asciz	"gettimeofday"                  ; string offset=481
	.asciz	"int"                           ; string offset=494
	.asciz	"timeval"                       ; string offset=498
	.asciz	"tv_sec"                        ; string offset=506
	.asciz	"__darwin_time_t"               ; string offset=513
	.asciz	"long"                          ; string offset=529
	.asciz	"tv_usec"                       ; string offset=534
	.asciz	"__darwin_suseconds_t"          ; string offset=542
	.asciz	"__int32_t"                     ; string offset=563
	.asciz	"dst"                           ; string offset=573
	.asciz	"outLen"                        ; string offset=577
	.asciz	"dstBufferLen"                  ; string offset=584
	.asciz	"src"                           ; string offset=597
	.asciz	"count"                         ; string offset=601
	.asciz	"i"                             ; string offset=607
	.asciz	"cur"                           ; string offset=609
	.asciz	"start"                         ; string offset=613
	.asciz	"end"                           ; string offset=619
	.asciz	"numberCount"                   ; string offset=623
	.asciz	"idx"                           ; string offset=635
	.asciz	"buf"                           ; string offset=639
	.asciz	"j"                             ; string offset=643
	.asciz	"dstLen"                        ; string offset=645
	.asciz	"runtimes"                      ; string offset=652
	.asciz	"outlen"                        ; string offset=661
	.asciz	"total"                         ; string offset=668
	.asciz	"datas"                         ; string offset=674
	.section	__DWARF,__apple_names,regular,debug
Lnames_begin:
	.long	1212240712                      ; Header Magic
	.short	1                               ; Header Version
	.short	0                               ; Header Hash Function
	.long	12                              ; Header Bucket Count
	.long	12                              ; Header Hash Count
	.long	12                              ; Header Data Length
	.long	0                               ; HeaderData Die Offset Base
	.long	1                               ; HeaderData Atom Count
	.short	1                               ; DW_ATOM_die_offset
	.short	6                               ; DW_FORM_data4
	.long	-1                              ; Bucket 0
	.long	0                               ; Bucket 1
	.long	-1                              ; Bucket 2
	.long	-1                              ; Bucket 3
	.long	1                               ; Bucket 4
	.long	4                               ; Bucket 5
	.long	5                               ; Bucket 6
	.long	6                               ; Bucket 7
	.long	-1                              ; Bucket 8
	.long	-1                              ; Bucket 9
	.long	8                               ; Bucket 10
	.long	-1                              ; Bucket 11
	.long	-1743021159                     ; Hash in Bucket 1
	.long	-1744350828                     ; Hash in Bucket 4
	.long	-1740613380                     ; Hash in Bucket 4
	.long	-573261720                      ; Hash in Bucket 4
	.long	5381                            ; Hash in Bucket 5
	.long	303403806                       ; Hash in Bucket 6
	.long	-1744099269                     ; Hash in Bucket 7
	.long	-1741943049                     ; Hash in Bucket 7
	.long	2090499946                      ; Hash in Bucket 10
	.long	-1745428938                     ; Hash in Bucket 10
	.long	-1741691490                     ; Hash in Bucket 10
	.long	-1739535270                     ; Hash in Bucket 10
.set Lset113, LNames4-Lnames_begin      ; Offset in Bucket 1
	.long	Lset113
.set Lset114, LNames11-Lnames_begin     ; Offset in Bucket 4
	.long	Lset114
.set Lset115, LNames7-Lnames_begin      ; Offset in Bucket 4
	.long	Lset115
.set Lset116, LNames3-Lnames_begin      ; Offset in Bucket 4
	.long	Lset116
.set Lset117, LNames0-Lnames_begin      ; Offset in Bucket 5
	.long	Lset117
.set Lset118, LNames10-Lnames_begin     ; Offset in Bucket 6
	.long	Lset118
.set Lset119, LNames6-Lnames_begin      ; Offset in Bucket 7
	.long	Lset119
.set Lset120, LNames2-Lnames_begin      ; Offset in Bucket 7
	.long	Lset120
.set Lset121, LNames8-Lnames_begin      ; Offset in Bucket 10
	.long	Lset121
.set Lset122, LNames1-Lnames_begin      ; Offset in Bucket 10
	.long	Lset122
.set Lset123, LNames9-Lnames_begin      ; Offset in Bucket 10
	.long	Lset123
.set Lset124, LNames5-Lnames_begin      ; Offset in Bucket 10
	.long	Lset124
LNames4:
	.long	311                             ; UintRange35Bit
	.long	1                               ; Num DIEs
	.long	157
	.long	0
LNames11:
	.long	281                             ; UintRange21Bit
	.long	1                               ; Num DIEs
	.long	115
	.long	0
LNames7:
	.long	356                             ; UintRange56Bit
	.long	1                               ; Num DIEs
	.long	220
	.long	0
LNames3:
	.long	224                             ; UintRange7Bit
	.long	1                               ; Num DIEs
	.long	50
	.long	0
LNames0:
	.long	411                             ;
	.long	1                               ; Num DIEs
	.long	262
	.long	0
LNames10:
	.long	459                             ; MarshalVarInt64s
	.long	1                               ; Num DIEs
	.long	354
	.long	0
LNames6:
	.long	296                             ; UintRange28Bit
	.long	1                               ; Num DIEs
	.long	136
	.long	0
LNames2:
	.long	326                             ; UintRange42Bit
	.long	1                               ; Num DIEs
	.long	178
	.long	0
LNames8:
	.long	476                             ; main
	.long	1                               ; Num DIEs
	.long	495
	.long	0
LNames1:
	.long	266                             ; UintRange14Bit
	.long	1                               ; Num DIEs
	.long	94
	.long	0
LNames9:
	.long	341                             ; UintRange49Bit
	.long	1                               ; Num DIEs
	.long	199
	.long	0
LNames5:
	.long	371                             ; UintRange63Bit
	.long	1                               ; Num DIEs
	.long	241
	.long	0
	.section	__DWARF,__apple_objc,regular,debug
Lobjc_begin:
	.long	1212240712                      ; Header Magic
	.short	1                               ; Header Version
	.short	0                               ; Header Hash Function
	.long	1                               ; Header Bucket Count
	.long	0                               ; Header Hash Count
	.long	12                              ; Header Data Length
	.long	0                               ; HeaderData Die Offset Base
	.long	1                               ; HeaderData Atom Count
	.short	1                               ; DW_ATOM_die_offset
	.short	6                               ; DW_FORM_data4
	.long	-1                              ; Bucket 0
	.section	__DWARF,__apple_namespac,regular,debug
Lnamespac_begin:
	.long	1212240712                      ; Header Magic
	.short	1                               ; Header Version
	.short	0                               ; Header Hash Function
	.long	1                               ; Header Bucket Count
	.long	0                               ; Header Hash Count
	.long	12                              ; Header Data Length
	.long	0                               ; HeaderData Die Offset Base
	.long	1                               ; HeaderData Atom Count
	.short	1                               ; DW_ATOM_die_offset
	.short	6                               ; DW_FORM_data4
	.long	-1                              ; Bucket 0
	.section	__DWARF,__apple_types,regular,debug
Ltypes_begin:
	.long	1212240712                      ; Header Magic
	.short	1                               ; Header Version
	.short	0                               ; Header Hash Function
	.long	15                              ; Header Bucket Count
	.long	15                              ; Header Hash Count
	.long	20                              ; Header Data Length
	.long	0                               ; HeaderData Die Offset Base
	.long	3                               ; HeaderData Atom Count
	.short	1                               ; DW_ATOM_die_offset
	.short	6                               ; DW_FORM_data4
	.short	3                               ; DW_ATOM_die_tag
	.short	5                               ; DW_FORM_data2
	.short	4                               ; DW_ATOM_type_flags
	.short	11                              ; DW_FORM_data1
	.long	-1                              ; Bucket 0
	.long	0                               ; Bucket 1
	.long	-1                              ; Bucket 2
	.long	2                               ; Bucket 3
	.long	5                               ; Bucket 4
	.long	-1                              ; Bucket 5
	.long	-1                              ; Bucket 6
	.long	-1                              ; Bucket 7
	.long	6                               ; Bucket 8
	.long	-1                              ; Bucket 9
	.long	8                               ; Bucket 10
	.long	9                               ; Bucket 11
	.long	10                              ; Bucket 12
	.long	11                              ; Bucket 13
	.long	12                              ; Bucket 14
	.long	789719536                       ; Hash in Bucket 1
	.long	-1921963995                     ; Hash in Bucket 1
	.long	1950534918                      ; Hash in Bucket 3
	.long	2090479413                      ; Hash in Bucket 3
	.long	-503405833                      ; Hash in Bucket 3
	.long	-243996567                      ; Hash in Bucket 4
	.long	193495088                       ; Hash in Bucket 8
	.long	-1622434163                     ; Hash in Bucket 8
	.long	857652610                       ; Hash in Bucket 10
	.long	-594775205                      ; Hash in Bucket 11
	.long	-1308701839                     ; Hash in Bucket 12
	.long	-113419488                      ; Hash in Bucket 13
	.long	290821634                       ; Hash in Bucket 14
	.long	2090147939                      ; Hash in Bucket 14
	.long	-104093792                      ; Hash in Bucket 14
.set Lset125, Ltypes5-Ltypes_begin      ; Offset in Bucket 1
	.long	Lset125
.set Lset126, Ltypes0-Ltypes_begin      ; Offset in Bucket 1
	.long	Lset126
.set Lset127, Ltypes1-Ltypes_begin      ; Offset in Bucket 3
	.long	Lset127
.set Lset128, Ltypes7-Ltypes_begin      ; Offset in Bucket 3
	.long	Lset128
.set Lset129, Ltypes8-Ltypes_begin      ; Offset in Bucket 3
	.long	Lset129
.set Lset130, Ltypes2-Ltypes_begin      ; Offset in Bucket 4
	.long	Lset130
.set Lset131, Ltypes4-Ltypes_begin      ; Offset in Bucket 8
	.long	Lset131
.set Lset132, Ltypes3-Ltypes_begin      ; Offset in Bucket 8
	.long	Lset132
.set Lset133, Ltypes13-Ltypes_begin     ; Offset in Bucket 10
	.long	Lset133
.set Lset134, Ltypes9-Ltypes_begin      ; Offset in Bucket 11
	.long	Lset134
.set Lset135, Ltypes6-Ltypes_begin      ; Offset in Bucket 12
	.long	Lset135
.set Lset136, Ltypes10-Ltypes_begin     ; Offset in Bucket 13
	.long	Lset136
.set Lset137, Ltypes12-Ltypes_begin     ; Offset in Bucket 14
	.long	Lset137
.set Lset138, Ltypes14-Ltypes_begin     ; Offset in Bucket 14
	.long	Lset138
.set Lset139, Ltypes11-Ltypes_begin     ; Offset in Bucket 14
	.long	Lset139
Ltypes5:
	.long	412                             ; uint8_t
	.long	1                               ; Num DIEs
	.long	305
	.short	22
	.byte	0
	.long	0
Ltypes0:
	.long	442                             ; long long
	.long	1                               ; Num DIEs
	.long	339
	.short	36
	.byte	0
	.long	0
Ltypes1:
	.long	563                             ; __int32_t
	.long	1                               ; Num DIEs
	.long	975
	.short	22
	.byte	0
	.long	0
Ltypes7:
	.long	529                             ; long
	.long	1                               ; Num DIEs
	.long	957
	.short	36
	.byte	0
	.long	0
Ltypes8:
	.long	498                             ; timeval
	.long	1                               ; Num DIEs
	.long	913
	.short	19
	.byte	0
	.long	0
Ltypes2:
	.long	513                             ; __darwin_time_t
	.long	1                               ; Num DIEs
	.long	946
	.short	22
	.byte	0
	.long	0
Ltypes4:
	.long	494                             ; int
	.long	1                               ; Num DIEs
	.long	896
	.short	36
	.byte	0
	.long	0
Ltypes3:
	.long	434                             ; int64_t
	.long	1                               ; Num DIEs
	.long	328
	.short	22
	.byte	0
	.long	0
Ltypes13:
	.long	247                             ; unsigned long long
	.long	1                               ; Num DIEs
	.long	87
	.short	36
	.byte	0
	.long	0
Ltypes9:
	.long	391                             ; __ARRAY_SIZE_TYPE__
	.long	1                               ; Num DIEs
	.long	298
	.short	36
	.byte	0
	.long	0
Ltypes6:
	.long	542                             ; __darwin_suseconds_t
	.long	1                               ; Num DIEs
	.long	964
	.short	22
	.byte	0
	.long	0
Ltypes10:
	.long	452                             ; double
	.long	1                               ; Num DIEs
	.long	347
	.short	36
	.byte	0
	.long	0
Ltypes12:
	.long	238                             ; uint64_t
	.long	1                               ; Num DIEs
	.long	76
	.short	22
	.byte	0
	.long	0
Ltypes14:
	.long	386                             ; char
	.long	1                               ; Num DIEs
	.long	291
	.short	36
	.byte	0
	.long	0
Ltypes11:
	.long	420                             ; unsigned char
	.long	1                               ; Num DIEs
	.long	316
	.short	36
	.byte	0
	.long	0
.subsections_via_symbols
	.section	__DWARF,__debug_line,regular,debug
Lsection_line:
Lline_table_start0:
