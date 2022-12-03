import os

num_tests = 30


if __name__ == "__main__":
    for i in range(num_tests):
        print("ROUND " + str(i))

        # with log
        file_name = "./logs/test" + str(i) + ".log"
        os.system(f"make cargo_test_test_snapshot_install_2d >{file_name} 2>&1")

        with open(file_name) as f:
            if 'FAILED' in f.read():
                print(file_name + " fails")
            else:
                print(file_name + " ok")
                os.system("rm " + file_name)

