import argparse
import unittest

from core.test.tests import get_suites, TestResultCompareFileMeld

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

    if args.test:
        if args.test == 'all':
            for s in suites:
                suites[s].run()
        else:
            if args.test not in list(suites.keys()):
                print('Test suite %s not found' %args.test)
            if args.meld:
                runner = unittest.TextTestRunner(verbosity=2, resultclass=TestResultCompareFileMeld).run(suites[args.test])
            else:
                runner = unittest.TextTestRunner(verbosity=2).run(suites[args.test])

