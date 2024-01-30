	.text
	.intel_syntax noprefix
	.file	"marshal_var_int64s.c"
	.file	0 "/Users/fuchunzhang/code/github.com/ahfuzhang/VictoriaMetrics_cluster_v1.96.0/lib/encoding/c" "marshal_var_int64s.c" md5 0x0819076236d44fe30bcd58064f4b3f48
	.file	1 "/usr/include/x86_64-linux-gnu/bits" "types.h" md5 0xd108b5f93a74c50510d7d9bc0ab36df9
	.file	2 "/usr/include/x86_64-linux-gnu/bits" "stdint-uintn.h" md5 0x2bf2ae53c58c01b1a1b9383b5195125c






// 函数开始
	.globl	MarshalVarInt64s                # -- Begin function MarshalVarInt64s
	.p2align	4, 0x90
	.type	MarshalVarInt64s,@function
MarshalVarInt64s:                       # @MarshalVarInt64s
.Lfunc_begin0:
	.loc	0 6 0                           # marshal_var_int64s.c:6:0
	.cfi_startproc
# %bb.0:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	push	rbx  // ???
	.cfi_def_cfa_offset 16
	.cfi_offset rbx, -16
.Ltmp0:
	#DEBUG_VALUE: i <- 0
	.loc	0 7 17 prologue_end             # marshal_var_int64s.c:7:17
	test	r8d, r8d  // 判断 count 是否为 0
	.loc	0 7 24 is_stmt 0                # marshal_var_int64s.c:7:24
	jle	.LBB0_19   // LBB0_19 是函数的结束位置
/*
在 Intel 汇编语言中，指令 jle .LBB0_19 是一条条件跳转指令，用于根据当前的标志寄存器状态决定是否跳转到指定的标签（在这个例子中是 .LBB0_19）。
*/
.Ltmp1:
# %bb.1:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- 0
	.loc	0 0 24                          # marshal_var_int64s.c:0:24
	movsxd	r10, edx  // dstBufferLen 赋值到  r10
	.loc	0 7 17                          # marshal_var_int64s.c:7:17
	mov	r8d, r8d  // 空指令，用于代码对齐
.Ltmp2:
	.loc	0 0 17                          # marshal_var_int64s.c:0:17
	xor	r11d, r11d  // r11 = 0
	lea	r9, [rip + .LJTI0_0]  // 根据地址取内容  LJTI0_0是跳转表的开始
	// r9 记录了跳转表的开始
/*
在 Intel 汇编语言中，指令 lea r9, [rip + .LJTI0_0] 用于将一个特定的内存地址加载到 r9 寄存器中。这条指令使用了 lea（Load Effective Address）操作，它常用于计算并加载地址而不是地址处的数据。

具体来说：

r9：这是一个 64 位的通用寄存器，在 AMD64 架构中用于各种目的，包括作为函数参数传递和临时存储值。
rip：这是一个特殊的寄存器，称为“指令指针寄存器”（在 64 位模式下称为 RIP），它总是包含当前执行指令的下一条指令的地址。
.LJTI0_0：这是一个标签，代表程序中某个特定位置的地址。在汇编代码中，标签通常用于表示跳转目标、数据位置或代码段的起点。
因此，lea r9, [rip + .LJTI0_0] 这条指令的作用是计算从当前指令的下一条指令到标签 .LJTI0_0 的偏移量，并将这个计算出的地址加载到 r9 寄存器中。这种方式常用于基于程序计数器的相对寻址，特别是在需要引用程序中其他位置的数据或函数时。例如，在创建跳转表或处理大型数据结构时，这种指令非常有用。
*/
	jmp	.LBB0_2  // 跳转到循环开始的位置
.Ltmp3:

// case 9
.LBB0_17:                               #   in Loop: Header=BB0_2 Depth=1  // case 9, 不是循环展开
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 98 14 is_stmt 1               # marshal_var_int64s.c:98:14
	mov	edx, eax  // eax 赋值给 edx
	or	dl, -128
	.loc	0 98 12 is_stmt 0               # marshal_var_int64s.c:98:12
	mov	byte ptr [rdi], dl  // 把寄存器的值写回地址
	.loc	0 99 28 is_stmt 1               # marshal_var_int64s.c:99:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 99 14 is_stmt 0               # marshal_var_int64s.c:99:14
	or	dl, -128
	.loc	0 99 12                         # marshal_var_int64s.c:99:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 100 28 is_stmt 1              # marshal_var_int64s.c:100:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 100 14 is_stmt 0              # marshal_var_int64s.c:100:14
	or	dl, -128
	.loc	0 100 12                        # marshal_var_int64s.c:100:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 101 28 is_stmt 1              # marshal_var_int64s.c:101:28
	mov	rdx, rax
	shr	rdx, 21
	.loc	0 101 14 is_stmt 0              # marshal_var_int64s.c:101:14
	or	dl, -128
	.loc	0 101 12                        # marshal_var_int64s.c:101:12
	mov	byte ptr [rdi + 3], dl
	.loc	0 102 28 is_stmt 1              # marshal_var_int64s.c:102:28
	mov	rdx, rax
	shr	rdx, 28
	.loc	0 102 14 is_stmt 0              # marshal_var_int64s.c:102:14
	or	dl, -128
	.loc	0 102 12                        # marshal_var_int64s.c:102:12
	mov	byte ptr [rdi + 4], dl
	.loc	0 103 28 is_stmt 1              # marshal_var_int64s.c:103:28
	mov	rdx, rax
	shr	rdx, 35
	.loc	0 103 14 is_stmt 0              # marshal_var_int64s.c:103:14
	or	dl, -128
	.loc	0 103 12                        # marshal_var_int64s.c:103:12
	mov	byte ptr [rdi + 5], dl
	.loc	0 104 28 is_stmt 1              # marshal_var_int64s.c:104:28
	mov	rdx, rax
	shr	rdx, 42
	.loc	0 104 14 is_stmt 0              # marshal_var_int64s.c:104:14
	or	dl, -128
	.loc	0 104 12                        # marshal_var_int64s.c:104:12
	mov	byte ptr [rdi + 6], dl
	.loc	0 105 28 is_stmt 1              # marshal_var_int64s.c:105:28
	mov	rdx, rax
	shr	rdx, 49
	.loc	0 105 14 is_stmt 0              # marshal_var_int64s.c:105:14
	or	dl, -128
	.loc	0 105 12                        # marshal_var_int64s.c:105:12
	mov	byte ptr [rdi + 7], dl
	.loc	0 106 28 is_stmt 1              # marshal_var_int64s.c:106:28
	mov	rdx, rax
	shr	rdx, 56
	.loc	0 106 14 is_stmt 0              # marshal_var_int64s.c:106:14
	or	dl, -128
	.loc	0 106 12                        # marshal_var_int64s.c:106:12
	mov	byte ptr [rdi + 8], dl
	.loc	0 107 28 is_stmt 1              # marshal_var_int64s.c:107:28
	shr	rax, 63
.Ltmp4:
	.loc	0 107 12 is_stmt 0              # marshal_var_int64s.c:107:12
	mov	byte ptr [rdi + 9], al
	.loc	0 108 12 is_stmt 1              # marshal_var_int64s.c:108:12
	add	qword ptr [rsi], 10
	.loc	0 109 8                         # marshal_var_int64s.c:109:8
	add	rdi, 10
.Ltmp5:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.p2align	4, 0x90

// 循环开始的位置
.LBB0_18:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	.loc	0 7 53                          # marshal_var_int64s.c:7:53
	add	r11, 1
.Ltmp6:
	#DEBUG_VALUE: i <- $r11
	.loc	0 7 17 is_stmt 0                # marshal_var_int64s.c:7:17
	cmp	r8, r11
	.loc	0 7 24                          # marshal_var_int64s.c:7:24
	je	.LBB0_19
.Ltmp7:

//??? 是什么   // 循环开始的位置
.LBB0_2:                                # =>This Inner Loop Header: Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	.loc	0 7 27                          # marshal_var_int64s.c:7:27
	mov	rax, qword ptr [rsi]
	.loc	0 7 34                          # marshal_var_int64s.c:7:34
	add	rax, 10
	.loc	0 7 37                          # marshal_var_int64s.c:7:37
	cmp	rax, r10  // r10 是 dstBufferLen
.Ltmp8:
	.loc	0 7 2                           # marshal_var_int64s.c:7:2
	jae	.LBB0_19
.Ltmp9:
# %bb.3:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11  // r11 是循环变量
	.loc	0 8 30 is_stmt 1                # marshal_var_int64s.c:8:30
	mov	rax, qword ptr [rcx + 8*r11]  // 从数组读取 int64 类型的整数
	.loc	0 8 37 is_stmt 0                # marshal_var_int64s.c:8:37
	lea	rdx, [rax + rax]
	.loc	0 8 53                          # marshal_var_int64s.c:8:53
	sar	rax, 63  // 右移 63 位
.Ltmp10:
	.loc	0 9 10 is_stmt 1                # marshal_var_int64s.c:9:10
	xor	rax, rdx  // int64 转换成  uint64
.Ltmp11:
	#DEBUG_VALUE: cur <- $rax
	.loc	0 9 7 is_stmt 0                 # marshal_var_int64s.c:9:7
	je	.LBB0_4


// 关键代码：计算跳转表中的偏移量
.Ltmp12:
# %bb.6:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 15 36 is_stmt 1               # marshal_var_int64s.c:15:36
	mov	rdx, rax
	shr	rdx
	.loc	0 15 17 is_stmt 0               # marshal_var_int64s.c:15:17
	bsr	rdx, rdx
	xor	edx, 63
	.loc	0 15 15                         # marshal_var_int64s.c:15:15
	mov	bl, 64
	sub	bl, dl
	.loc	0 15 41                         # marshal_var_int64s.c:15:41
	movzx	edx, bl
	lea	ebx, [rdx + 8*rdx]
	lea	ebx, [rdx + 4*rbx]
	shr	ebx, 8
	sub	dl, bl
	shr	dl
	add	dl, bl
	shr	dl, 2
	.loc	0 15 3                          # marshal_var_int64s.c:15:3
	cmp	dl, 9
	ja	.LBB0_18
.Ltmp13:
# %bb.7:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 0 3                           # marshal_var_int64s.c:0:3
	movzx	edx, dl
	movsxd	rdx, dword ptr [r9 + 4*rdx]  // r9 是跳转表
	// 为什么指令只有四字节
	add	rdx, r9
	jmp	rdx
.Ltmp14:

// case 0
.LBB0_8:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 17 12 is_stmt 1               # marshal_var_int64s.c:17:12
	mov	byte ptr [rdi], al
	jmp	.LBB0_5
.Ltmp15:
	.p2align	4, 0x90
.LBB0_4:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 10 9                          # marshal_var_int64s.c:10:9
	mov	byte ptr [rdi], 0
.Ltmp16:
.LBB0_5:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 0 0 is_stmt 0                 # marshal_var_int64s.c:0:0
	add	rsi, 8
.Ltmp17:
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	add	rdi, 1
.Ltmp18:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	jmp	.LBB0_18  // 循环开始的位置
.Ltmp19:

// case 1
.LBB0_9:                                #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 22 14 is_stmt 1               # marshal_var_int64s.c:22:14
	mov	edx, eax
	or	dl, -128
	.loc	0 22 12 is_stmt 0               # marshal_var_int64s.c:22:12
	mov	byte ptr [rdi], dl
	.loc	0 23 27 is_stmt 1               # marshal_var_int64s.c:23:27
	shr	rax, 7
.Ltmp20:
	.loc	0 23 12 is_stmt 0               # marshal_var_int64s.c:23:12
	mov	byte ptr [rdi + 1], al
	.loc	0 24 12 is_stmt 1               # marshal_var_int64s.c:24:12
	add	qword ptr [rsi], 2
	.loc	0 25 8                          # marshal_var_int64s.c:25:8
	add	rdi, 2
.Ltmp21:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18  // 跳到循环开始的位置
.Ltmp22:

// case 2
.LBB0_10:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 28 14 is_stmt 1               # marshal_var_int64s.c:28:14
	mov	edx, eax
	or	dl, -128
	.loc	0 28 12 is_stmt 0               # marshal_var_int64s.c:28:12
	mov	byte ptr [rdi], dl
	.loc	0 29 28 is_stmt 1               # marshal_var_int64s.c:29:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 29 14 is_stmt 0               # marshal_var_int64s.c:29:14
	or	dl, -128
	.loc	0 29 12                         # marshal_var_int64s.c:29:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 30 28 is_stmt 1               # marshal_var_int64s.c:30:28
	shr	rax, 14
.Ltmp23:
	.loc	0 30 12 is_stmt 0               # marshal_var_int64s.c:30:12
	mov	byte ptr [rdi + 2], al
	.loc	0 31 12 is_stmt 1               # marshal_var_int64s.c:31:12
	add	qword ptr [rsi], 3
	.loc	0 32 8                          # marshal_var_int64s.c:32:8
	add	rdi, 3
.Ltmp24:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp25:

// case 3
.LBB0_11:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 35 14 is_stmt 1               # marshal_var_int64s.c:35:14
	mov	edx, eax
	or	dl, -128
	.loc	0 35 12 is_stmt 0               # marshal_var_int64s.c:35:12
	mov	byte ptr [rdi], dl
	.loc	0 36 28 is_stmt 1               # marshal_var_int64s.c:36:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 36 14 is_stmt 0               # marshal_var_int64s.c:36:14
	or	dl, -128
	.loc	0 36 12                         # marshal_var_int64s.c:36:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 37 28 is_stmt 1               # marshal_var_int64s.c:37:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 37 14 is_stmt 0               # marshal_var_int64s.c:37:14
	or	dl, -128
	.loc	0 37 12                         # marshal_var_int64s.c:37:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 38 28 is_stmt 1               # marshal_var_int64s.c:38:28
	shr	rax, 21
.Ltmp26:
	.loc	0 38 12 is_stmt 0               # marshal_var_int64s.c:38:12
	mov	byte ptr [rdi + 3], al
	.loc	0 39 12 is_stmt 1               # marshal_var_int64s.c:39:12
	add	qword ptr [rsi], 4
	.loc	0 40 8                          # marshal_var_int64s.c:40:8
	add	rdi, 4
.Ltmp27:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp28:

// case 4
.LBB0_12:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 43 14 is_stmt 1               # marshal_var_int64s.c:43:14
	mov	edx, eax
	or	dl, -128
	.loc	0 43 12 is_stmt 0               # marshal_var_int64s.c:43:12
	mov	byte ptr [rdi], dl
	.loc	0 44 28 is_stmt 1               # marshal_var_int64s.c:44:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 44 14 is_stmt 0               # marshal_var_int64s.c:44:14
	or	dl, -128
	.loc	0 44 12                         # marshal_var_int64s.c:44:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 45 28 is_stmt 1               # marshal_var_int64s.c:45:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 45 14 is_stmt 0               # marshal_var_int64s.c:45:14
	or	dl, -128
	.loc	0 45 12                         # marshal_var_int64s.c:45:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 46 28 is_stmt 1               # marshal_var_int64s.c:46:28
	mov	rdx, rax
	shr	rdx, 21
	.loc	0 46 14 is_stmt 0               # marshal_var_int64s.c:46:14
	or	dl, -128
	.loc	0 46 12                         # marshal_var_int64s.c:46:12
	mov	byte ptr [rdi + 3], dl
	.loc	0 47 28 is_stmt 1               # marshal_var_int64s.c:47:28
	shr	rax, 28
.Ltmp29:
	.loc	0 47 12 is_stmt 0               # marshal_var_int64s.c:47:12
	mov	byte ptr [rdi + 4], al
	.loc	0 48 12 is_stmt 1               # marshal_var_int64s.c:48:12
	add	qword ptr [rsi], 5
	.loc	0 49 8                          # marshal_var_int64s.c:49:8
	add	rdi, 5
.Ltmp30:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp31:

// case 5
.LBB0_13:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 52 14 is_stmt 1               # marshal_var_int64s.c:52:14
	mov	edx, eax
	or	dl, -128
	.loc	0 52 12 is_stmt 0               # marshal_var_int64s.c:52:12
	mov	byte ptr [rdi], dl
	.loc	0 53 28 is_stmt 1               # marshal_var_int64s.c:53:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 53 14 is_stmt 0               # marshal_var_int64s.c:53:14
	or	dl, -128
	.loc	0 53 12                         # marshal_var_int64s.c:53:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 54 28 is_stmt 1               # marshal_var_int64s.c:54:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 54 14 is_stmt 0               # marshal_var_int64s.c:54:14
	or	dl, -128
	.loc	0 54 12                         # marshal_var_int64s.c:54:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 55 28 is_stmt 1               # marshal_var_int64s.c:55:28
	mov	rdx, rax
	shr	rdx, 21
	.loc	0 55 14 is_stmt 0               # marshal_var_int64s.c:55:14
	or	dl, -128
	.loc	0 55 12                         # marshal_var_int64s.c:55:12
	mov	byte ptr [rdi + 3], dl
	.loc	0 56 28 is_stmt 1               # marshal_var_int64s.c:56:28
	mov	rdx, rax
	shr	rdx, 28
	.loc	0 56 14 is_stmt 0               # marshal_var_int64s.c:56:14
	or	dl, -128
	.loc	0 56 12                         # marshal_var_int64s.c:56:12
	mov	byte ptr [rdi + 4], dl
	.loc	0 57 28 is_stmt 1               # marshal_var_int64s.c:57:28
	shr	rax, 35
.Ltmp32:
	.loc	0 57 12 is_stmt 0               # marshal_var_int64s.c:57:12
	mov	byte ptr [rdi + 5], al
	.loc	0 58 12 is_stmt 1               # marshal_var_int64s.c:58:12
	add	qword ptr [rsi], 6
	.loc	0 59 8                          # marshal_var_int64s.c:59:8
	add	rdi, 6
.Ltmp33:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp34:

// case 6
.LBB0_14:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 62 14 is_stmt 1               # marshal_var_int64s.c:62:14
	mov	edx, eax
	or	dl, -128
	.loc	0 62 12 is_stmt 0               # marshal_var_int64s.c:62:12
	mov	byte ptr [rdi], dl
	.loc	0 63 28 is_stmt 1               # marshal_var_int64s.c:63:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 63 14 is_stmt 0               # marshal_var_int64s.c:63:14
	or	dl, -128
	.loc	0 63 12                         # marshal_var_int64s.c:63:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 64 28 is_stmt 1               # marshal_var_int64s.c:64:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 64 14 is_stmt 0               # marshal_var_int64s.c:64:14
	or	dl, -128
	.loc	0 64 12                         # marshal_var_int64s.c:64:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 65 28 is_stmt 1               # marshal_var_int64s.c:65:28
	mov	rdx, rax
	shr	rdx, 21
	.loc	0 65 14 is_stmt 0               # marshal_var_int64s.c:65:14
	or	dl, -128
	.loc	0 65 12                         # marshal_var_int64s.c:65:12
	mov	byte ptr [rdi + 3], dl
	.loc	0 66 28 is_stmt 1               # marshal_var_int64s.c:66:28
	mov	rdx, rax
	shr	rdx, 28
	.loc	0 66 14 is_stmt 0               # marshal_var_int64s.c:66:14
	or	dl, -128
	.loc	0 66 12                         # marshal_var_int64s.c:66:12
	mov	byte ptr [rdi + 4], dl
	.loc	0 67 28 is_stmt 1               # marshal_var_int64s.c:67:28
	mov	rdx, rax
	shr	rdx, 35
	.loc	0 67 14 is_stmt 0               # marshal_var_int64s.c:67:14
	or	dl, -128
	.loc	0 67 12                         # marshal_var_int64s.c:67:12
	mov	byte ptr [rdi + 5], dl
	.loc	0 68 28 is_stmt 1               # marshal_var_int64s.c:68:28
	shr	rax, 42
.Ltmp35:
	.loc	0 68 12 is_stmt 0               # marshal_var_int64s.c:68:12
	mov	byte ptr [rdi + 6], al
	.loc	0 69 12 is_stmt 1               # marshal_var_int64s.c:69:12
	add	qword ptr [rsi], 7
	.loc	0 70 8                          # marshal_var_int64s.c:70:8
	add	rdi, 7
.Ltmp36:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp37:

// case 7
.LBB0_15:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 73 14 is_stmt 1               # marshal_var_int64s.c:73:14
	mov	edx, eax
	or	dl, -128
	.loc	0 73 12 is_stmt 0               # marshal_var_int64s.c:73:12
	mov	byte ptr [rdi], dl
	.loc	0 74 28 is_stmt 1               # marshal_var_int64s.c:74:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 74 14 is_stmt 0               # marshal_var_int64s.c:74:14
	or	dl, -128
	.loc	0 74 12                         # marshal_var_int64s.c:74:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 75 28 is_stmt 1               # marshal_var_int64s.c:75:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 75 14 is_stmt 0               # marshal_var_int64s.c:75:14
	or	dl, -128
	.loc	0 75 12                         # marshal_var_int64s.c:75:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 76 28 is_stmt 1               # marshal_var_int64s.c:76:28
	mov	rdx, rax
	shr	rdx, 21
	.loc	0 76 14 is_stmt 0               # marshal_var_int64s.c:76:14
	or	dl, -128
	.loc	0 76 12                         # marshal_var_int64s.c:76:12
	mov	byte ptr [rdi + 3], dl
	.loc	0 77 28 is_stmt 1               # marshal_var_int64s.c:77:28
	mov	rdx, rax
	shr	rdx, 28
	.loc	0 77 14 is_stmt 0               # marshal_var_int64s.c:77:14
	or	dl, -128
	.loc	0 77 12                         # marshal_var_int64s.c:77:12
	mov	byte ptr [rdi + 4], dl
	.loc	0 78 28 is_stmt 1               # marshal_var_int64s.c:78:28
	mov	rdx, rax
	shr	rdx, 35
	.loc	0 78 14 is_stmt 0               # marshal_var_int64s.c:78:14
	or	dl, -128
	.loc	0 78 12                         # marshal_var_int64s.c:78:12
	mov	byte ptr [rdi + 5], dl
	.loc	0 79 28 is_stmt 1               # marshal_var_int64s.c:79:28
	mov	rdx, rax
	shr	rdx, 42
	.loc	0 79 14 is_stmt 0               # marshal_var_int64s.c:79:14
	or	dl, -128
	.loc	0 79 12                         # marshal_var_int64s.c:79:12
	mov	byte ptr [rdi + 6], dl
	.loc	0 80 28 is_stmt 1               # marshal_var_int64s.c:80:28
	shr	rax, 49
.Ltmp38:
	.loc	0 80 12 is_stmt 0               # marshal_var_int64s.c:80:12
	mov	byte ptr [rdi + 7], al
	.loc	0 81 12 is_stmt 1               # marshal_var_int64s.c:81:12
	add	qword ptr [rsi], 8
	.loc	0 82 8                          # marshal_var_int64s.c:82:8
	add	rdi, 8
.Ltmp39:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp40:

// case 8
.LBB0_16:                               #   in Loop: Header=BB0_2 Depth=1
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	#DEBUG_VALUE: i <- $r11
	#DEBUG_VALUE: cur <- $rax
	.loc	0 85 14 is_stmt 1               # marshal_var_int64s.c:85:14
	mov	edx, eax
	or	dl, -128
	.loc	0 85 12 is_stmt 0               # marshal_var_int64s.c:85:12
	mov	byte ptr [rdi], dl
	.loc	0 86 28 is_stmt 1               # marshal_var_int64s.c:86:28
	mov	rdx, rax
	shr	rdx, 7
	.loc	0 86 14 is_stmt 0               # marshal_var_int64s.c:86:14
	or	dl, -128
	.loc	0 86 12                         # marshal_var_int64s.c:86:12
	mov	byte ptr [rdi + 1], dl
	.loc	0 87 28 is_stmt 1               # marshal_var_int64s.c:87:28
	mov	rdx, rax
	shr	rdx, 14
	.loc	0 87 14 is_stmt 0               # marshal_var_int64s.c:87:14
	or	dl, -128
	.loc	0 87 12                         # marshal_var_int64s.c:87:12
	mov	byte ptr [rdi + 2], dl
	.loc	0 88 28 is_stmt 1               # marshal_var_int64s.c:88:28
	mov	rdx, rax
	shr	rdx, 21
	.loc	0 88 14 is_stmt 0               # marshal_var_int64s.c:88:14
	or	dl, -128
	.loc	0 88 12                         # marshal_var_int64s.c:88:12
	mov	byte ptr [rdi + 3], dl
	.loc	0 89 28 is_stmt 1               # marshal_var_int64s.c:89:28
	mov	rdx, rax
	shr	rdx, 28
	.loc	0 89 14 is_stmt 0               # marshal_var_int64s.c:89:14
	or	dl, -128
	.loc	0 89 12                         # marshal_var_int64s.c:89:12
	mov	byte ptr [rdi + 4], dl
	.loc	0 90 28 is_stmt 1               # marshal_var_int64s.c:90:28
	mov	rdx, rax
	shr	rdx, 35
	.loc	0 90 14 is_stmt 0               # marshal_var_int64s.c:90:14
	or	dl, -128
	.loc	0 90 12                         # marshal_var_int64s.c:90:12
	mov	byte ptr [rdi + 5], dl
	.loc	0 91 28 is_stmt 1               # marshal_var_int64s.c:91:28
	mov	rdx, rax
	shr	rdx, 42
	.loc	0 91 14 is_stmt 0               # marshal_var_int64s.c:91:14
	or	dl, -128
	.loc	0 91 12                         # marshal_var_int64s.c:91:12
	mov	byte ptr [rdi + 6], dl
	.loc	0 92 28 is_stmt 1               # marshal_var_int64s.c:92:28
	mov	rdx, rax
	shr	rdx, 49
	.loc	0 92 14 is_stmt 0               # marshal_var_int64s.c:92:14
	or	dl, -128
	.loc	0 92 12                         # marshal_var_int64s.c:92:12
	mov	byte ptr [rdi + 7], dl
	.loc	0 93 28 is_stmt 1               # marshal_var_int64s.c:93:28
	shr	rax, 56
.Ltmp41:
	.loc	0 93 12 is_stmt 0               # marshal_var_int64s.c:93:12
	mov	byte ptr [rdi + 8], al
	.loc	0 94 12 is_stmt 1               # marshal_var_int64s.c:94:12
	add	qword ptr [rsi], 9
	.loc	0 95 8                          # marshal_var_int64s.c:95:8
	add	rdi, 9
.Ltmp42:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	.loc	0 0 8 is_stmt 0                 # marshal_var_int64s.c:0:8
	jmp	.LBB0_18
.Ltmp43:

// 这里是函数的结束位置
.LBB0_19:
	#DEBUG_VALUE: MarshalVarInt64s:dst <- $rdi
	#DEBUG_VALUE: MarshalVarInt64s:outLen <- $rsi
	#DEBUG_VALUE: MarshalVarInt64s:dstBufferLen <- [DW_OP_LLVM_entry_value 1] $edx
	#DEBUG_VALUE: MarshalVarInt64s:src <- $rcx
	#DEBUG_VALUE: MarshalVarInt64s:count <- $r8d
	.loc	0 113 1 is_stmt 1               # marshal_var_int64s.c:113:1
	pop	rbx
	.cfi_def_cfa_offset 8
	ret
.Ltmp44:
.Lfunc_end0:
	.size	MarshalVarInt64s, .Lfunc_end0-MarshalVarInt64s
	.cfi_endproc
	.section	.rodata,"a",@progbits
	.p2align	2
.LJTI0_0:  // 这里定义了跳转表
	.long	.LBB0_8-.LJTI0_0  //??? 不理解减法是干什么
	.long	.LBB0_9-.LJTI0_0
	.long	.LBB0_10-.LJTI0_0
	.long	.LBB0_11-.LJTI0_0
	.long	.LBB0_12-.LJTI0_0
	.long	.LBB0_13-.LJTI0_0
	.long	.LBB0_14-.LJTI0_0
	.long	.LBB0_15-.LJTI0_0
	.long	.LBB0_16-.LJTI0_0
	.long	.LBB0_17-.LJTI0_0  // 这里跳到  case 9
                                        # -- End function














	.file	3 "/usr/include/x86_64-linux-gnu/bits" "stdint-intn.h" md5 0x55bcbdc3159515ebd91d351a70d505f4
	.section	.debug_loclists,"",@progbits
	.long	.Ldebug_list_header_end0-.Ldebug_list_header_start0 # Length
.Ldebug_list_header_start0:
	.short	5                               # Version
	.byte	8                               # Address size
	.byte	0                               # Segment selector size
	.long	4                               # Offset entry count
.Lloclists_table_base0:
	.long	.Ldebug_loc0-.Lloclists_table_base0
	.long	.Ldebug_loc1-.Lloclists_table_base0
	.long	.Ldebug_loc2-.Lloclists_table_base0
	.long	.Ldebug_loc3-.Lloclists_table_base0
.Ldebug_loc0:
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Lfunc_begin0-.Lfunc_begin0    #   starting offset
	.uleb128 .Ltmp3-.Lfunc_begin0           #   ending offset
	.byte	1                               # Loc expr size
	.byte	81                              # super-register DW_OP_reg1
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp3-.Lfunc_begin0           #   starting offset
	.uleb128 .Lfunc_end0-.Lfunc_begin0      #   ending offset
	.byte	4                               # Loc expr size
	.byte	163                             # DW_OP_entry_value
	.byte	1                               # 1
	.byte	81                              # super-register DW_OP_reg1
	.byte	159                             # DW_OP_stack_value
	.byte	0                               # DW_LLE_end_of_list
.Ldebug_loc1:
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Lfunc_begin0-.Lfunc_begin0    #   starting offset
	.uleb128 .Ltmp2-.Lfunc_begin0           #   ending offset
	.byte	1                               # Loc expr size
	.byte	88                              # super-register DW_OP_reg8
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp3-.Lfunc_begin0           #   starting offset
	.uleb128 .Lfunc_end0-.Lfunc_begin0      #   ending offset
	.byte	1                               # Loc expr size
	.byte	88                              # super-register DW_OP_reg8
	.byte	0                               # DW_LLE_end_of_list
.Ldebug_loc2:
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp0-.Lfunc_begin0           #   starting offset
	.uleb128 .Ltmp3-.Lfunc_begin0           #   ending offset
	.byte	3                               # Loc expr size
	.byte	17                              # DW_OP_consts
	.byte	0                               # 0
	.byte	159                             # DW_OP_stack_value
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp3-.Lfunc_begin0           #   starting offset
	.uleb128 .Ltmp43-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	91                              # DW_OP_reg11
	.byte	0                               # DW_LLE_end_of_list
.Ldebug_loc3:
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp3-.Lfunc_begin0           #   starting offset
	.uleb128 .Ltmp4-.Lfunc_begin0           #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp11-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp20-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp22-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp23-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp25-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp26-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp28-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp29-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp31-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp32-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp34-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp35-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp37-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp38-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	4                               # DW_LLE_offset_pair
	.uleb128 .Ltmp40-.Lfunc_begin0          #   starting offset
	.uleb128 .Ltmp41-.Lfunc_begin0          #   ending offset
	.byte	1                               # Loc expr size
	.byte	80                              # DW_OP_reg0
	.byte	0                               # DW_LLE_end_of_list
.Ldebug_list_header_end0:
	.section	.debug_abbrev,"",@progbits
	.byte	1                               # Abbreviation Code
	.byte	17                              # DW_TAG_compile_unit
	.byte	1                               # DW_CHILDREN_yes
	.byte	37                              # DW_AT_producer
	.byte	37                              # DW_FORM_strx1
	.byte	19                              # DW_AT_language
	.byte	5                               # DW_FORM_data2
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	114                             # DW_AT_str_offsets_base
	.byte	23                              # DW_FORM_sec_offset
	.byte	16                              # DW_AT_stmt_list
	.byte	23                              # DW_FORM_sec_offset
	.byte	27                              # DW_AT_comp_dir
	.byte	37                              # DW_FORM_strx1
	.byte	17                              # DW_AT_low_pc
	.byte	27                              # DW_FORM_addrx
	.byte	18                              # DW_AT_high_pc
	.byte	6                               # DW_FORM_data4
	.byte	115                             # DW_AT_addr_base
	.byte	23                              # DW_FORM_sec_offset
	.byte	116                             # DW_AT_rnglists_base
	.byte	23                              # DW_FORM_sec_offset
	.ascii	"\214\001"                      # DW_AT_loclists_base
	.byte	23                              # DW_FORM_sec_offset
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	2                               # Abbreviation Code
	.byte	22                              # DW_TAG_typedef
	.byte	0                               # DW_CHILDREN_no
	.byte	73                              # DW_AT_type
	.byte	19                              # DW_FORM_ref4
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	58                              # DW_AT_decl_file
	.byte	11                              # DW_FORM_data1
	.byte	59                              # DW_AT_decl_line
	.byte	11                              # DW_FORM_data1
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	3                               # Abbreviation Code
	.byte	36                              # DW_TAG_base_type
	.byte	0                               # DW_CHILDREN_no
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	62                              # DW_AT_encoding
	.byte	11                              # DW_FORM_data1
	.byte	11                              # DW_AT_byte_size
	.byte	11                              # DW_FORM_data1
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	4                               # Abbreviation Code
	.byte	46                              # DW_TAG_subprogram
	.byte	1                               # DW_CHILDREN_yes
	.byte	17                              # DW_AT_low_pc
	.byte	27                              # DW_FORM_addrx
	.byte	18                              # DW_AT_high_pc
	.byte	6                               # DW_FORM_data4
	.byte	64                              # DW_AT_frame_base
	.byte	24                              # DW_FORM_exprloc
	.byte	122                             # DW_AT_call_all_calls
	.byte	25                              # DW_FORM_flag_present
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	58                              # DW_AT_decl_file
	.byte	11                              # DW_FORM_data1
	.byte	59                              # DW_AT_decl_line
	.byte	11                              # DW_FORM_data1
	.byte	39                              # DW_AT_prototyped
	.byte	25                              # DW_FORM_flag_present
	.byte	63                              # DW_AT_external
	.byte	25                              # DW_FORM_flag_present
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	5                               # Abbreviation Code
	.byte	5                               # DW_TAG_formal_parameter
	.byte	0                               # DW_CHILDREN_no
	.byte	2                               # DW_AT_location
	.byte	24                              # DW_FORM_exprloc
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	58                              # DW_AT_decl_file
	.byte	11                              # DW_FORM_data1
	.byte	59                              # DW_AT_decl_line
	.byte	11                              # DW_FORM_data1
	.byte	73                              # DW_AT_type
	.byte	19                              # DW_FORM_ref4
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	6                               # Abbreviation Code
	.byte	5                               # DW_TAG_formal_parameter
	.byte	0                               # DW_CHILDREN_no
	.byte	2                               # DW_AT_location
	.byte	34                              # DW_FORM_loclistx
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	58                              # DW_AT_decl_file
	.byte	11                              # DW_FORM_data1
	.byte	59                              # DW_AT_decl_line
	.byte	11                              # DW_FORM_data1
	.byte	73                              # DW_AT_type
	.byte	19                              # DW_FORM_ref4
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	7                               # Abbreviation Code
	.byte	11                              # DW_TAG_lexical_block
	.byte	1                               # DW_CHILDREN_yes
	.byte	17                              # DW_AT_low_pc
	.byte	27                              # DW_FORM_addrx
	.byte	18                              # DW_AT_high_pc
	.byte	6                               # DW_FORM_data4
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	8                               # Abbreviation Code
	.byte	52                              # DW_TAG_variable
	.byte	0                               # DW_CHILDREN_no
	.byte	2                               # DW_AT_location
	.byte	34                              # DW_FORM_loclistx
	.byte	3                               # DW_AT_name
	.byte	37                              # DW_FORM_strx1
	.byte	58                              # DW_AT_decl_file
	.byte	11                              # DW_FORM_data1
	.byte	59                              # DW_AT_decl_line
	.byte	11                              # DW_FORM_data1
	.byte	73                              # DW_AT_type
	.byte	19                              # DW_FORM_ref4
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	9                               # Abbreviation Code
	.byte	11                              # DW_TAG_lexical_block
	.byte	1                               # DW_CHILDREN_yes
	.byte	85                              # DW_AT_ranges
	.byte	35                              # DW_FORM_rnglistx
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	10                              # Abbreviation Code
	.byte	15                              # DW_TAG_pointer_type
	.byte	0                               # DW_CHILDREN_no
	.byte	73                              # DW_AT_type
	.byte	19                              # DW_FORM_ref4
	.byte	0                               # EOM(1)
	.byte	0                               # EOM(2)
	.byte	0                               # EOM(3)
	.section	.debug_info,"",@progbits
.Lcu_begin0:
	.long	.Ldebug_info_end0-.Ldebug_info_start0 # Length of Unit
.Ldebug_info_start0:
	.short	5                               # DWARF version number
	.byte	1                               # DWARF Unit Type
	.byte	8                               # Address Size (in bytes)
	.long	.debug_abbrev                   # Offset Into Abbrev. Section
	.byte	1                               # Abbrev [1] 0xc:0xc7 DW_TAG_compile_unit
	.byte	0                               # DW_AT_producer
	.short	12                              # DW_AT_language
	.byte	1                               # DW_AT_name
	.long	.Lstr_offsets_base0             # DW_AT_str_offsets_base
	.long	.Lline_table_start0             # DW_AT_stmt_list
	.byte	2                               # DW_AT_comp_dir
	.byte	0                               # DW_AT_low_pc
	.long	.Lfunc_end0-.Lfunc_begin0       # DW_AT_high_pc
	.long	.Laddr_table_base0              # DW_AT_addr_base
	.long	.Lrnglists_table_base0          # DW_AT_rnglists_base
	.long	.Lloclists_table_base0          # DW_AT_loclists_base
	.byte	2                               # Abbrev [2] 0x2b:0x8 DW_TAG_typedef
	.long	51                              # DW_AT_type
	.byte	5                               # DW_AT_name
	.byte	2                               # DW_AT_decl_file
	.byte	27                              # DW_AT_decl_line
	.byte	2                               # Abbrev [2] 0x33:0x8 DW_TAG_typedef
	.long	59                              # DW_AT_type
	.byte	4                               # DW_AT_name
	.byte	1                               # DW_AT_decl_file
	.byte	45                              # DW_AT_decl_line
	.byte	3                               # Abbrev [3] 0x3b:0x4 DW_TAG_base_type
	.byte	3                               # DW_AT_name
	.byte	7                               # DW_AT_encoding
	.byte	8                               # DW_AT_byte_size
	.byte	2                               # Abbrev [2] 0x3f:0x8 DW_TAG_typedef
	.long	71                              # DW_AT_type
	.byte	8                               # DW_AT_name
	.byte	2                               # DW_AT_decl_file
	.byte	24                              # DW_AT_decl_line
	.byte	2                               # Abbrev [2] 0x47:0x8 DW_TAG_typedef
	.long	79                              # DW_AT_type
	.byte	7                               # DW_AT_name
	.byte	1                               # DW_AT_decl_file
	.byte	38                              # DW_AT_decl_line
	.byte	3                               # Abbrev [3] 0x4f:0x4 DW_TAG_base_type
	.byte	6                               # DW_AT_name
	.byte	8                               # DW_AT_encoding
	.byte	1                               # DW_AT_byte_size
	.byte	4                               # Abbrev [4] 0x53:0x58 DW_TAG_subprogram
	.byte	0                               # DW_AT_low_pc
	.long	.Lfunc_end0-.Lfunc_begin0       # DW_AT_high_pc
	.byte	1                               # DW_AT_frame_base
	.byte	87
                                        # DW_AT_call_all_calls
	.byte	9                               # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	6                               # DW_AT_decl_line
                                        # DW_AT_prototyped
                                        # DW_AT_external
	.byte	5                               # Abbrev [5] 0x5e:0xa DW_TAG_formal_parameter
	.byte	1                               # DW_AT_location
	.byte	85
	.byte	10                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	6                               # DW_AT_decl_line
	.long	171                             # DW_AT_type
	.byte	5                               # Abbrev [5] 0x68:0xa DW_TAG_formal_parameter
	.byte	1                               # DW_AT_location
	.byte	84
	.byte	11                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	6                               # DW_AT_decl_line
	.long	176                             # DW_AT_type
	.byte	6                               # Abbrev [6] 0x72:0x9 DW_TAG_formal_parameter
	.byte	0                               # DW_AT_location
	.byte	12                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	6                               # DW_AT_decl_line
	.long	181                             # DW_AT_type
	.byte	5                               # Abbrev [5] 0x7b:0xa DW_TAG_formal_parameter
	.byte	1                               # DW_AT_location
	.byte	82
	.byte	14                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	6                               # DW_AT_decl_line
	.long	185                             # DW_AT_type
	.byte	6                               # Abbrev [6] 0x85:0x9 DW_TAG_formal_parameter
	.byte	1                               # DW_AT_location
	.byte	18                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	6                               # DW_AT_decl_line
	.long	181                             # DW_AT_type
	.byte	7                               # Abbrev [7] 0x8e:0x1c DW_TAG_lexical_block
	.byte	0                               # DW_AT_low_pc
	.long	.Ltmp43-.Lfunc_begin0           # DW_AT_high_pc
	.byte	8                               # Abbrev [8] 0x94:0x9 DW_TAG_variable
	.byte	2                               # DW_AT_location
	.byte	19                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	7                               # DW_AT_decl_line
	.long	181                             # DW_AT_type
	.byte	9                               # Abbrev [9] 0x9d:0xc DW_TAG_lexical_block
	.byte	0                               # DW_AT_ranges
	.byte	8                               # Abbrev [8] 0x9f:0x9 DW_TAG_variable
	.byte	3                               # DW_AT_location
	.byte	20                              # DW_AT_name
	.byte	0                               # DW_AT_decl_file
	.byte	8                               # DW_AT_decl_line
	.long	43                              # DW_AT_type
	.byte	0                               # End Of Children Mark
	.byte	0                               # End Of Children Mark
	.byte	0                               # End Of Children Mark
	.byte	10                              # Abbrev [10] 0xab:0x5 DW_TAG_pointer_type
	.long	63                              # DW_AT_type
	.byte	10                              # Abbrev [10] 0xb0:0x5 DW_TAG_pointer_type
	.long	43                              # DW_AT_type
	.byte	3                               # Abbrev [3] 0xb5:0x4 DW_TAG_base_type
	.byte	13                              # DW_AT_name
	.byte	5                               # DW_AT_encoding
	.byte	4                               # DW_AT_byte_size
	.byte	10                              # Abbrev [10] 0xb9:0x5 DW_TAG_pointer_type
	.long	190                             # DW_AT_type
	.byte	2                               # Abbrev [2] 0xbe:0x8 DW_TAG_typedef
	.long	198                             # DW_AT_type
	.byte	17                              # DW_AT_name
	.byte	3                               # DW_AT_decl_file
	.byte	27                              # DW_AT_decl_line
	.byte	2                               # Abbrev [2] 0xc6:0x8 DW_TAG_typedef
	.long	206                             # DW_AT_type
	.byte	16                              # DW_AT_name
	.byte	1                               # DW_AT_decl_file
	.byte	44                              # DW_AT_decl_line
	.byte	3                               # Abbrev [3] 0xce:0x4 DW_TAG_base_type
	.byte	15                              # DW_AT_name
	.byte	5                               # DW_AT_encoding
	.byte	8                               # DW_AT_byte_size
	.byte	0                               # End Of Children Mark
.Ldebug_info_end0:
	.section	.debug_rnglists,"",@progbits
	.long	.Ldebug_list_header_end1-.Ldebug_list_header_start1 # Length
.Ldebug_list_header_start1:
	.short	5                               # Version
	.byte	8                               # Address size
	.byte	0                               # Segment selector size
	.long	1                               # Offset entry count
.Lrnglists_table_base0:
	.long	.Ldebug_ranges0-.Lrnglists_table_base0
.Ldebug_ranges0:
	.byte	4                               # DW_RLE_offset_pair
	.uleb128 .Ltmp3-.Lfunc_begin0           #   starting offset
	.uleb128 .Ltmp5-.Lfunc_begin0           #   ending offset
	.byte	4                               # DW_RLE_offset_pair
	.uleb128 .Ltmp9-.Lfunc_begin0           #   starting offset
	.uleb128 .Ltmp43-.Lfunc_begin0          #   ending offset
	.byte	0                               # DW_RLE_end_of_list
.Ldebug_list_header_end1:
	.section	.debug_str_offsets,"",@progbits
	.long	88                              # Length of String Offsets Set
	.short	5
	.short	0
.Lstr_offsets_base0:
	.section	.debug_str,"MS",@progbits,1
.Linfo_string0:
	.asciz	"Debian clang version 14.0.6"   # string offset=0
.Linfo_string1:
	.asciz	"marshal_var_int64s.c"          # string offset=28
.Linfo_string2:
	.asciz	"/Users/fuchunzhang/code/github.com/ahfuzhang/VictoriaMetrics_cluster_v1.96.0/lib/encoding/c" # string offset=49
.Linfo_string3:
	.asciz	"unsigned long"                 # string offset=141
.Linfo_string4:
	.asciz	"__uint64_t"                    # string offset=155
.Linfo_string5:
	.asciz	"uint64_t"                      # string offset=166
.Linfo_string6:
	.asciz	"unsigned char"                 # string offset=175
.Linfo_string7:
	.asciz	"__uint8_t"                     # string offset=189
.Linfo_string8:
	.asciz	"uint8_t"                       # string offset=199
.Linfo_string9:
	.asciz	"MarshalVarInt64s"              # string offset=207
.Linfo_string10:
	.asciz	"dst"                           # string offset=224
.Linfo_string11:
	.asciz	"outLen"                        # string offset=228
.Linfo_string12:
	.asciz	"dstBufferLen"                  # string offset=235
.Linfo_string13:
	.asciz	"int"                           # string offset=248
.Linfo_string14:
	.asciz	"src"                           # string offset=252
.Linfo_string15:
	.asciz	"long"                          # string offset=256
.Linfo_string16:
	.asciz	"__int64_t"                     # string offset=261
.Linfo_string17:
	.asciz	"int64_t"                       # string offset=271
.Linfo_string18:
	.asciz	"count"                         # string offset=279
.Linfo_string19:
	.asciz	"i"                             # string offset=285
.Linfo_string20:
	.asciz	"cur"                           # string offset=287
	.section	.debug_str_offsets,"",@progbits
	.long	.Linfo_string0
	.long	.Linfo_string1
	.long	.Linfo_string2
	.long	.Linfo_string3
	.long	.Linfo_string4
	.long	.Linfo_string5
	.long	.Linfo_string6
	.long	.Linfo_string7
	.long	.Linfo_string8
	.long	.Linfo_string9
	.long	.Linfo_string10
	.long	.Linfo_string11
	.long	.Linfo_string12
	.long	.Linfo_string13
	.long	.Linfo_string14
	.long	.Linfo_string15
	.long	.Linfo_string16
	.long	.Linfo_string17
	.long	.Linfo_string18
	.long	.Linfo_string19
	.long	.Linfo_string20
	.section	.debug_addr,"",@progbits
	.long	.Ldebug_addr_end0-.Ldebug_addr_start0 # Length of contribution
.Ldebug_addr_start0:
	.short	5                               # DWARF version number
	.byte	8                               # Address size
	.byte	0                               # Segment selector size
.Laddr_table_base0:
	.quad	.Lfunc_begin0
.Ldebug_addr_end0:
	.ident	"Debian clang version 14.0.6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
	.section	.debug_line,"",@progbits
.Lline_table_start0:
