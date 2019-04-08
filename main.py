import argparse
import importlib
import unittest

from test.tests import get_suites, TestResultCompareFileMeld

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TLDB')
    suites = get_suites()
    parser.add_argument('--test',
                        help=f"Test all or a specific suite among: {'|'.join(list(suites.keys()) + ['all'])} "
                        f"or a specific test cases",
                        type=str,
                        required=True)
    parser.add_argument('--verbosity',
                        help=f"Test verbosity (1 or 2)",
                        type=int,
                        required=False,
                        default=2)
    parser.add_argument('--meld',
                        help='Use meld to compare out and exp file (True or False_',
                        type=str,
                        default='true',
                        required=False)

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        exit()

    if args.meld.lower() == 'true':
        use_meld = True
    else:
        use_meld = False
    if use_meld:
        result_class = TestResultCompareFileMeld
    else:
        result_class = unittest.TextTestResult

    runner = unittest.TextTestRunner(verbosity=args.verbosity, resultclass=result_class)

    if args.test:
        if args.test == 'all':
            for s in suites:
                runner.run(suites[s])
        else:
            if args.test in list(suites.keys()):
                runner.run(suites[args.test])
            else:
                try:
                    test_path = args.test.split('.')
                    module = importlib.import_module('.'.join(test_path[:-1]))
                    test_case_class = getattr(module, test_path[-1])
                    suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case_class)
                    runner.run(suite)
                except ValueError:
                    print(f"Suite or test case {args.test} not found")

