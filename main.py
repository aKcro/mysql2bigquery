# -*- coding: utf-8 -*-
import argparse
import settings
import core.bqengine as bqengine
import core.myengine as myengine
import traceback
import sys


def main(args):
    try:
        filename = myengine.dump_query(args.sourcedb, args.query)
        bqengine.prepare_dataset(args.dataset, args.target)
        bqengine.load_file(filename, args.dataset, args.target)

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        print(e)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--query', help='sync a query', default='')
    parser.add_argument('--sourcedb', help='source database', default='')
    parser.add_argument('--dataset', help='target dataset', default='')
    parser.add_argument('--target', help='target table', default='')
    args = parser.parse_args()
    main(args)