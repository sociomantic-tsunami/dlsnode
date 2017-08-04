#!/usr/bin/env python

import os
import sys
import time
import struct
import os.path
import argparse

size_t_fmt = 'Q'
size_t_sizeof = struct.calcsize(size_t_fmt)


def main():
    args = parse_args()

    if args.consistency_check or args.repair:
        consistency_checker = ConsistencyChecker(args)
        consistency_checker.check(args.consistency_check or args.repair)
        sys.exit(0)

    if args.size_check:
        size_checker = SizeChecker(args)
        size_checker.check(args.size_check)
        sys.exit(0)

    # Read
    if args.generate:
        generator = Generator(args)
        info = generator.generate(args.generate)
    elif args.read:
        reader = Reader(args)
        info = reader.read(args.read)

    if args.add_from:
        reader = Reader(args)
        info += reader.read(args.add_from)

    # Write
    if args.write:
        writer = Writer(args)
        writer.write(args.write, info)
    sys.stdout.write('%s\n' % info)


class ConsistencyChecker:
    def __init__(self, args):
        self.args = args

    def check(self, path):
        if not os.path.isdir(path):
            range = self.get_range(path)
            if range is None:
                sys.exit(3)
            r, e = self.process_file(path, range[0], range[1])
            echo("%s: %s records processed, %s errors", path, r, e)
            return
        files = 0
        ferrors = 0
        records = 0
        errors = 0
        for fname in os.listdir(path):
            full_fname = os.path.join(path, fname)
            if not os.path.isdir(full_fname):
                continue
            f, fe, r, re = self.process_dir(full_fname)
            files += f
            ferrors += fe
            records += r
            errors += re
        pct = 0
        if files > 0:
            pct = 100.0*ferrors/files
        echo("%s: %s files processed, %s files with errors (%.2f%%), "
                "%s records processed in total (%s errors)",
                path, files, ferrors, pct, records, errors)

    def process_dir(self, dirname):
        stime = time.time()
        files = 0
        ferrors = 0
        records = 0
        errors = 0
        timestamp = '0x' + os.path.basename(dirname)
        for fname in os.listdir(dirname):
            if fname.endswith('.gz'):
                continue
            full_fname = os.path.join(dirname, fname)
            range = self.get_range(full_fname)
            if range is None:
                self.warn('%s: skipped', full_fname)
                continue
            ts = int(timestamp + fname, 16)
            if self.args.block_filter and not \
                    eval('ts ' + self.args.block_filter):
                echo("%s %s %s", ts, self.args.block_filter,
                        'FALSE')
                continue
            if self.args.block_filter:
                echo("%s %s %s", ts, self.args.block_filter,
                        'TRUE')
            r, e = self.process_file(full_fname, range[0], range[1])
            records += r
            errors += e
            files += 1
            if e > 0:
                ferrors += 1
            etime = time.time()
            if  etime - stime > 2:
                echo("%s: processing... (%s files done)",
                        full_fname, files)
                stime = etime
        return files, ferrors, records, errors

    def process_file(self, fname, min_key, max_key):
        with file(fname) as fin:
            (read, errors) = self.check_file(fin, min_key, max_key)
        if not errors or not self.args.repair:
            return read, errors
        rep_fname = fname + '.repairing'
        with file(fname) as fin, file(rep_fname, 'w') as fout:
            rec = self.repair_file(fin, fout, min_key, max_key)
            fout.flush()
        os.rename(fname, fname + '.broken')
        os.rename(rep_fname, fname)
        return rec, errors

    def check_file(self, fo, min_key, max_key):
        key = 0
        records_read = 0
        errors = 0
        while True:
            try:
                data = fo.read(size_t_sizeof)
            except Exception as e:
                self.warn('%s: error reading key, skipping '
                    'file after successfully reading %s '
                    'records (last read key was 0x%016x)',
                    fo.name, records_read, key)
                errors += 1
                continue
            if not data:
                break
            try:
                key = struct.unpack(size_t_fmt, data)[0]
            except Exception as e:
                self.warn('%s: error unpacking key from bytes '
                    '%r, skipping file after successfully '
                    'reading %s records (last read key was '
                    '0x%016x)',
                    fo.name, data, records_read, key)
                errors += 1
                continue
            if key < min_key or key > max_key:
                self.warn('%s: key 0x%016x out of range '
                    '(0x%016x, 0x%016x), %s records '
                    'read already', fo.name,
                    key, min_key, max_key, records_read)
                errors += 1
                continue
            try:
                data = fo.read(size_t_sizeof)
            except Exception as e:
                self.warn('%s: error reading value length, '
                    'skipping file after successfully '
                    'reading %s records (last read key '
                    'was 0x%016x)',
                    fo.name, records_read, key)
                errors += 1
                continue
            try:
                length = struct.unpack(size_t_fmt, data)[0]
            except Exception as e:
                self.warn('%s: error unpacking value length '
                    'for key 0x%016x (bytes %r), skipping '
                    'file after successfully reading %s '
                    'records',
                    fo.name, key, data, records_read)
                errors += 1
                continue
            try:
                value = fo.read(length) # relative
            except Exception as e:
                self.warn('%s: error seeking the length of the '
                    'value (%s) for key 0x%016x, skipping  '
                    'file aftersuccessfully reading %s '
                    'records',
                    fo.name, length, key, records_read)
                errors += 1
                continue
            records_read += 1

        return (records_read, errors)

    def repair_file(self, fin, fout, min_key, max_key):
        records_written = 0
        while True:
            try:
                key_bytes = fin.read(size_t_sizeof)
                if not key_bytes:
                    break
                key = struct.unpack(size_t_fmt, key_bytes)[0]
                if key < min_key or key > max_key:
                    continue
                length_bytes = fin.read(size_t_sizeof)
                length = struct.unpack(size_t_fmt, length_bytes)[0]
                value_bytes = fin.read(length)
            except Exception as e:
                continue
            fout.write(key_bytes)
            fout.write(length_bytes)
            fout.write(value_bytes)
            records_written += 1
        return records_written

    def get_range(self, path):
        path_components = path.split('/')[-2:]
        if len(path_components) < 2:
            self.warn("%s: the path must include the directory "
                    "to check the range", path)
            return None
        dirname, fname = path_components
        ok = True
        ok &= self.check_hexa_fname(path, 'directory', dirname, 10)
        ok &= self.check_hexa_fname(path, 'file', fname, 3)
        if not ok:
            return None
        min = int(dirname+fname+'000', 16)
        return min, min + 0xfff

    def check_hexa_fname(self, path, type, fname, length):
        if len(fname) != length:
            self.warn("%s: the %s name '%s' should have length %s",
                    path, fname, type, length)
        try:
            int(fname, 16)
        except ValueError as e:
            self.warn("%s: the %s name '%s' is not a valid "
                    "base 16 number",
                    path, fname, type)
            return False
        return True

    def warn(self, fmt, *args):
        sys.stderr.write((fmt + '\n') % args)


class SizeChecker:
    def __init__(self, args):
        self.args = args

    def check(self, path):
        expected = Generator(self.args).generate(path)
        real = Reader(self.args).read(os.path.join(path, 'sizeinfo'))

        if real != expected:
            sys.stderr.write('%s: expected=[%s] current=[%s] '
                    '(diff=[%s])\n' % (path, expected, real,
                            expected - real))
            sys.exit(1)


class Generator:
    def __init__(self, args):
        self.args = args
        self.info = SizeInfo()

    def generate(self, path):
        for fname in os.listdir(path):
            full_fname = os.path.join(path, fname)
            if not os.path.isdir(full_fname):
                continue
            self.process_dir(full_fname)
        return self.info

    def process_dir(self, dirname):
        timestamp = '0x' + os.path.basename(dirname)
        for fname in os.listdir(dirname):
            if fname.endswith('.gz'):
                continue
            try:
                ts = int(timestamp + fname, 16)
            except ValueError as e:
                sys.stderr.write('%s: file name format '
                    'unrecognized, not in hexa, '
                    'skipping\n', timestamp + fname)
                continue
            if self.args.block_filter and not \
                    eval('ts ' + self.args.block_filter):
                echo("%s %s %s", ts, self.args.block_filter,
                        'FALSE')
                continue
            if self.args.block_filter:
                echo("%s %s %s", ts, self.args.block_filter,
                        'TRUE')
            full_fname = os.path.join(dirname, fname)
            with file(full_fname) as fo:
                self.process_file(fo)

    def process_file(self, fo):
        while True:
            data = fo.read(size_t_sizeof)
            if not data:
                break
            key = struct.unpack(size_t_fmt, data)[0]
            length = struct.unpack(size_t_fmt, fo.read(size_t_sizeof))[0]
            fo.seek(length, 1) # relative
            self.info.size += length
            self.info.records += 1


class Reader:
    def __init__(self, args):
        self.args = args

    def read(self, path):
        info = SizeInfo()
        with file(path) as fo:
            info.records = self.read_size_t(fo)
            info.size = self.read_size_t(fo)
        return info

    def read_size_t(self, fo):
        return struct.unpack(size_t_fmt, fo.read(size_t_sizeof))[0]


class Writer:
    def __init__(self, args):
        self.args = args

    def write(self, path, info):
        with file(path, 'w') as fo:
            self.write_size_t(fo, info.records)
            self.write_size_t(fo, info.size)

    def write_size_t(self, fo, value):
        return fo.write(struct.pack(size_t_fmt, value))


class SizeInfo:
    def __init__(self):
        self.size = 0
        self.records = 0
    def __eq__(self, other):
        return self.size == other.size and self.records == other.records
    def __ne__(self, other):
        return not (self == other)
    def __add__(self, other):
        result = SizeInfo()
        result.size = self.size + other.size
        result.records = self.records + other.records
        return result
    def __sub__(self, other):
        result = SizeInfo()
        result.size = self.size - other.size
        result.records = self.records - other.records
        return result
    def __str__(self):
        return 'records=%s size=%s' % (self.records, self.size)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Log DHT sizeinfo manipulation tool')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--consistency-check', metavar='PATH',
        help='Check block files for consistency. PATH should be a '
        'channel directory or a single block file, in which case you '
        'have to specify the directory where is contained to extract '
        'the key range that file is handling. This checks that all the '
        'keys stored in the block file belongs to the file')
    group.add_argument('-R', '--repair', metavar='PATH',
        help='Repair consistency errors detected by --consistency-check '
        '(when possible)')
    group.add_argument('-s', '--size-check', metavar='PATH',
        help='Check if the size info from the channel stored in '
        'PATH is correct')
    group.add_argument('-g', '--generate', metavar='PATH',
        help='Generate the size info from the channel stored in PATH')
    group.add_argument('-r', '--read', metavar='SIZEINFO',
        help='Read the size info from the SIZEINFO file')

    parser.add_argument('-w', '--write', metavar='SIZEINFO',
        help='Write the size info to the SIZEINFO file')
    parser.add_argument('-f', '--block-filter', metavar='FILTER',
        help='Only process files that match FILTER (expressed in '
        'Python code where the value to compare to is the '
        'concatenation of the directory and the file name, converted '
        'to an integer. For example: "> 0x52123" will match the file '
        'chan/0000000052/125 but not the file chan/0000000052/123)')
    parser.add_argument('-a', '--add-from', metavar='SIZEINFO',
        help='Add to the read size info the contents of this '
        'SIZEINFO file')

    return parser.parse_args()


def echo(fmt, *args):
    sys.stdout.write((fmt + '\n') % args)
    sys.stdout.flush()


if __name__ == '__main__':
    main()
