import argparse
import unittest

from test.tests import get_suites, TestResultCompareFileMeld

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TLDB')
    suites = get_suites()
    parser.add_argument('--test',
                        help='Test all or a specific suite among: ' + ' | '.join(list(suites.keys()) + ['all']),
                        type=str,
                        required=False)
    parser.add_argument('--meld',
                        help='Use meld to compare out and exp file',
                        type=bool,
                        default=False,
                        required=False)

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        exit()

    if args.meld:
        result_class = TestResultCompareFileMeld
    else:
        result_class = unittest.TextTestResult

    if args.test:
        if args.test == 'all':
            for s in suites:
                runner = unittest.TextTestRunner(verbosity=2, resultclass=result_class).run(suites[s])
        else:
            if args.test not in list(suites.keys()):
                print('Test suite %s not found' %args.test)
            runner = unittest.TextTestRunner(verbosity=2, resultclass=result_class).run(suites[args.test])

