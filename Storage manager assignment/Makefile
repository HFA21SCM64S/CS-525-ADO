test_assign1 : test_assign1_1.o storage_mgr.o dberror.o
	cc -o test_assign1 test_assign1_1.o storage_mgr.o dberror.o
test_assign1_1.o : test_assign1_1.c storage_mgr.h test_helper.h dberror.h
	cc -c test_assign1_1.c
storage_mgr.o : storage_mgr.c storage_mgr.h
	cc -c storage_mgr.c
dberror.o : dberror.c dberror.h
	cc -c dberror.c
clean :
	rm test_assign1 test_assign1_1.o storage_mgr.o dberror.o 
