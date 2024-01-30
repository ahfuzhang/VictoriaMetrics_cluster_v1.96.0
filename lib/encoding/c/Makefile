
build:
	clang -o m marshal_var_int64s.c -g -Wall -O3

s:
	clang -S -o marshal_var_int64s.S -masm=att marshal_var_int64s.c -g -Wall -O3

intel:
	clang -S -o marshal_var_int64s.S -masm=intel marshal_var_int64s.c -g -Wall -O3

linux_amd64:
	clang --target=x86_64-linux-gnu -S -o linux_amd64.S -masm=intel marshal_var_int64s.c -g -Wall -O3 -DNO_MAIN
arm64:
	clang -S -o arm64.S -masm=intel marshal_var_int64s.c -Wall -O3 -DNO_MAIN
