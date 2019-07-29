import argparse
import importlib
import sys

from test.test_case import *
from test.test_result import *


class ArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


def get_test_suites(starting_dir: Path):
    checking_dirs = {starting_dir}
    suites_dir = set()
    while checking_dirs:
        checking_d = checking_dirs.pop()
        sub_dirs = {d for d in checking_d.iterdir() if d.is_dir() and d.stem != '__pycache__'}
        if not sub_dirs:
            suites_dir.add(checking_d)
        else:
            checking_dirs = checking_dirs.union(sub_dirs)
    test_suites = {}
    for d in suites_dir:
        tests = unittest.TestLoader().discover(d)
        if tests.countTestCases() > 0:
            parent = d.parent.stem
            test_suites[f"{parent}.{d.stem}"] = tests
    return test_suites


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TLDB tester')

    parser.add_argument('--verbosity', choices=[1, 2], help=f"Test verbosity (default 2)", type=int, default=2)
    parser.add_argument('--meld',
                        help='Use meld to compare out and exp file (default False)',
                        action='store_true')
    sub_parser = parser.add_subparsers(help="Input testing option", dest='testOption')
    t_suites = get_test_suites(root_path() / 'test')
    one_parser = sub_parser.add_parser('one', help='Test an existing suite or case')
    one_parser.add_argument('testName', type=str,
                            help=f"Input a suite among {list(t_suites.keys())} or a specific test case")
    all_parser = sub_parser.add_parser('all', help='Test all existing cases')

    args = parser.parse_args()
    if args.meld:
        result_class = TestResultCompareFileMeld
    else:
        result_class = TestResultLogMetrics

    # Create test out folder
    (root_path() / 'test' / 'io' / 'out').mkdir(parents=True, exist_ok=True)
    metrics_path = root_path() / 'test' / 'io' / 'out' / 'metrics.log'
    if metrics_path.is_file():
        with metrics_path.open(mode='a+') as f:
            f.write("=" * 20)
    runner = unittest.TextTestRunner(verbosity=args.verbosity, resultclass=result_class)

    result = False
    if args.testOption:
        if args.testOption == 'all':
            results = set()
            for s in t_suites:
                results.add(runner.run(t_suites[s]).wasSuccessful())
            result = all(results)
        elif args.testOption == 'one':
            if args.testName in list(t_suites.keys()):
                result = runner.run(t_suites[args.testName]).wasSuccessful()
            else:
                try:
                    test_path = args.testName.split('.')
                    module = importlib.import_module('.'.join(test_path[:-1]))
                    test_case_class = getattr(module, test_path[-1])
                    suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case_class)
                    result = runner.run(suite).wasSuccessful()
                except ValueError:
                    sys.stderr.write(f"error: Suite or test case {args.testName} not found\n")
                    parser.print_help()
        if not result:
            sys.exit("Some tests failed")
    else:
        sys.stderr.write(f"error: Testing option is required\n")
        parser.print_help()
        sys.exit(2)