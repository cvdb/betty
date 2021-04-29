import argparse
import os


def ProcessArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--do', help='the command to run')
    parser.add_argument('--command', help='the args to pass to the command')
    return parser.parse_args()

if __name__ == '__main__':
    args = ProcessArgs()

    if args.do == 'menu':
        print_menu()
    elif args.do == 'run':
        print('run')
    else:
        print('invalid selection')



