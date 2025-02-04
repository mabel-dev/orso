def run_tests():
    import contextlib
    import inspect
    from io import StringIO
    import shutil
    import time
    import traceback

    display_width = shutil.get_terminal_size((80, 20))[0]

    # Get the calling module
    caller_module = inspect.getmodule(inspect.currentframe().f_back)
    test_methods = []
    for name, obj in inspect.getmembers(caller_module):
        if inspect.isfunction(obj) and name.startswith("test_"):
            test_methods.append(obj)

    print(f"\n\033[38;2;139;233;253m\033[3mRUNNING SET OF {len(test_methods)} TESTS\033[0m\n")

    passed = 0
    failed = 0

    for index, method in enumerate(test_methods):
        start_time = time.monotonic_ns()
        test_name = f"\033[38;2;255;184;108m{(index + 1):04}\033[0m \033[38;2;189;147;249m{str(method.__name__)}\033[0m"
        print(test_name.ljust(display_width - 20), end="", flush=True)
        error = None
        output = ""
        try:
            stdout = StringIO()  # Create a StringIO object
            with contextlib.redirect_stdout(stdout):
                method()
            output = stdout.getvalue()
        except Exception as err:
            error = err
        finally:
            if error is None:
                passed += 1
                status = "\033[38;2;26;185;67m pass"
            else:
                failed += 1
                status = f"\033[38;2;255;121;198m fail"
        time_taken = int((time.monotonic_ns() - start_time) / 1e6)
        print(f"\033[0;32m{str(time_taken).rjust(8)}ms {status}\033[0m")
        if error:
            traceback_details = traceback.extract_tb(error.__traceback__)
            file_name, line_number, function_name, code_line = traceback_details[-1]
            file_name = file_name.split("/")[-1]
            print(
                f"  \033[38;2;255;121;198m{error.__class__.__name__}\033[0m"
                + f" {error}\n"
                + f"  \033[38;2;241;250;140m{file_name}\033[0m"
                + f"\033[38;2;98;114;164m:\033[0m"
                + f"\033[38;2;26;185;67m{line_number}\033[0m"
                + f" \033[38;2;98;114;164m{code_line}\033[0m"
            )
        if output:
            print(
                "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
                + output.strip()
                + "\n"
                + "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
            )

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m\n"
        f"  \033[38;2;26;185;67m{passed} passed\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
