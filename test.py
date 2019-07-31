import argparse
import importlib
import sys

from tests.test_case import *
from tests.test_result import *


METRICS_LOGS_PATH = root_path() / 'tests' / 'io' / 'out' / 'metrics.log'


class ArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


def generate_parser():
    example_text = """example:
    python test.py all
    python test.py all --testDirPath tests/testDir
    python test.py suite tests.testSuite --verbose -- meld
    python test.py case tests.testDir.testFile.testClass
    python test.py list tests
    """
    parser = argparse.ArgumentParser(description='Tester', epilog=example_text)
    parser.add_argument('--testDirPath',
                        help=f"Specify path to test dir containing tests from project root (default: tests)",
                        type=str,
                        default='tests')
    parser.add_argument('--metricsLogPath',
                        help=f"Specify path to metrics log file from project root (default: tests/io/out/metrics.log)",
                        type=str)
    parser.add_argument('--verbosity', choices=[1, 2], help=f"Test verbosity (default 2)", type=int, default=2)
    parser.add_argument('--meld',
                        help='Use meld to compare out and exp file (default False)',
                        action='store_true')
    sub_parser = parser.add_subparsers(help="Input testing option", dest='testOption')
    suite_parser = sub_parser.add_parser('suite', help='Test an existing suite')
    suite_parser.add_argument('suiteName', type=str, help=f"Name of the suite. Use list to see existing suite")
    case_parser = sub_parser.add_parser('case', help='Test an existing test case using')
    case_parser.add_argument('caseName', type=str, help="Path to test class. E.g: test/testDir/testFile/testClass")
    all_parser = sub_parser.add_parser('all', help='Test all existing cases from a testDir')
    list_parser = sub_parser.add_parser('listSuite', help='List test suites in a dir')
    list_parser.add_argument('dirPath', help='path to dir from project root')
    return parser


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


def init_metrics_log_file():
    METRICS_LOGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if METRICS_LOGS_PATH.is_file():
        with METRICS_LOGS_PATH.open(mode='a+') as f:
            f.write('='*20)


if __name__ == '__main__':
    test_parser = generate_parser()
    args = test_parser.parse_args()
    if args.meld:
        result_class = TestResultCompareFileMeld
    else:
        result_class = TestResultLogMetrics

    runner = unittest.TextTestRunner(verbosity=args.verbosity, resultclass=result_class)
    result = False
    if args.testOption:
        if args.metricsLogPath:
            METRICS_LOGS_PATH = root_path().joinpath(args.metricsLogPath)
        init_metrics_log_file()

        if args.testOption == 'all':
            results = set()
            t_suites = get_test_suites(root_path().joinpath(args.testDirPath))
            for s in t_suites:
                results.add(runner.run(t_suites[s]).wasSuccessful())
            result = all(results)
        elif args.testOption == 'suite':
            t_suites = get_test_suites(root_path().joinpath(args.testDirPath))
            if args.suiteName in list(t_suites.keys()):
                result = runner.run(t_suites[args.suiteName]).wasSuccessful()
            else:
                sys.stderr.write(f"error: Suite {args.suiteName} not found in test dir {args.testDirPath}.\n"
                                 f"Try another test suite or test dir.\n")
                test_parser.print_help()
        elif args.testOption == 'case':
            try:
                test_path = args.caseName.split('.')
                module = importlib.import_module('.'.join(test_path[:-1]))
                test_case_class = getattr(module, test_path[-1])
                suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case_class)
                result = runner.run(suite).wasSuccessful()
            except ValueError:
                sys.stderr.write(f"error: Suite {args.caseName} not found\n")
                test_parser.print_help()
        elif args.testOption == 'listSuite':
            t_suites = get_test_suites(root_path().joinpath(args.dirPath))
            if not t_suites:
                sys.stdout.write(f"No test suite found in {args.dirPath}")
            else:
                sys.stdout.write(f"{' '.join(t_suites.keys())}")
            result = True
        if not result:
            sys.exit("Some tests failed")
    else:
        sys.stderr.write(f"error: Testing option is required\n")
        test_parser.print_help()
        sys.exit(2)