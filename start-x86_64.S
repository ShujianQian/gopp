	.global start_routine
start_routine:
	movq %rdi, %rsp
	movq %rdx, %rdi
	movq $0, %rbp
	pushq %rsi
	ret
