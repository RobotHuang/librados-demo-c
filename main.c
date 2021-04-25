//
// Created by Huang on 2021.
//
#include <stdio.h>
#include <stdlib.h>
#include <rados/librados.h>

int main() {
    int ret = 0;
    const char* pool_name = "hello_world_pool";
    const char* hello_world = "HelloWorld";
    const char* object_name = "HelloWorld";
    rados_ioctx_t io_ctx = NULL;
    int pool_created = 0;

    rados_t rados = NULL;
    {
        ret = rados_create2(&rados, "ceph", "client.admin", 0);
        if (ret < 0) { // let's handle any error that might have come back
            printf("couldn't initialize rados! error %d\n", ret);
            ret = EXIT_FAILURE;
            goto out;
        }
        printf("we just set up a rados cluster object\n");
    }

    {
        ret = rados_conf_read_file(rados, "/etc/ceph/ceph.conf");
        if (ret < 0) {
            printf("couldn't read rados conf! error %d\n", ret);
            ret = EXIT_FAILURE;
            goto out;
        }
        printf("we just read a rados cluster conf\n");
    }

    {
        ret = rados_connect(rados);
        if (ret < 0) {
            printf("couldn't connect to cluster! error %d\n", ret);
            ret = EXIT_FAILURE;
            goto out;
        }
        printf("we just connected to the rados cluster\n");
    }

    {
        ret = rados_pool_create(rados, pool_name);
        if (ret < 0) {
            printf("couldn't create pool! error %d\n", ret);
            return EXIT_FAILURE;
        }
        printf("we just created a new pool named %s\n", pool_name);
        pool_created = 1;
    }

    {
        ret = rados_ioctx_create(rados, pool_name, &io_ctx);
        if (ret < 0) {
            printf("couldn't set up ioctx! error %d\n", ret);
            ret = EXIT_FAILURE;
            goto out;
        }
        printf("we just created an ioctx for our pool\n");
    }

    {
        ret = rados_write_full(io_ctx, object_name, hello_world, 10);
        if (ret < 0) {
            printf("couldn't write object! error %d\n", ret);
            ret = EXIT_FAILURE;
            goto out;
        }
        printf("we just wrote new object %s, with contents '%s'\n", object_name, hello_world);
    }

    {
        int read_len = 4194304; // this is way more than we need
        char* read_buf = malloc(read_len + 1); // add one for the terminating 0 we'll add later
        rados_read_op_t read_op = rados_create_read_op();
        size_t num = 0;
        rados_read_op_read(read_op, 0, strlen(hello_world), read_buf, &num, &ret);
        rados_read_op_set_flags(read_op, LIBRADOS_OPERATION_BALANCE_READS);
        ret = rados_read_op_operate(read_op, io_ctx, object_name, 0);
//        int num = rados_read(io_ctx, object_name, read_buf, read_len+1, 0);
        read_buf[num] = 0; // null-terminate the string
        printf("we read our object %s, and got back %d bytes with contents\n%s\n", object_name, num, read_buf);
        rados_release_read_op(read_op);
    }

    ret = EXIT_SUCCESS;
    out:
    if (io_ctx) {
        rados_ioctx_destroy(io_ctx);
    }

    if (pool_created) {
        /*
         * And now we're done, so let's remove our pool and then
         * shut down the connection gracefully.
         */
        int delete_ret = rados_pool_delete(rados, pool_name);
        if (delete_ret < 0) {
            // be careful not to
            printf("We failed to delete our test pool!\n");
            ret = EXIT_FAILURE;
        }
    }

    rados_shutdown(rados);
    return ret;
}
