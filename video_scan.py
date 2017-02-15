#!/usr/bin/python

import csv
import json
import os
import shlex
import subprocess
import sys
import time
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool


################################################################################
## Config

import signal

include_ext   = (                   # File types to match
    '.mov',
    '.mp4',
    '.mpg'
    '.avi',
    '.m4v',
    '.mkv',
    '.mpeg',
    '.qt',
    '.r3d'
#    '.dat'
)
copy_results  = True                # Whether to copy to clipboard afterwards (will overwrite clipboard!)
csv_file_name = 'scan_output.csv'   # Output file
use_threads   = True                # Whether to use threading (might prevent exiting the program until completion)
thread_count  = 8                   # Number of threads. Ideally matches the number of CPU cores (4-16)


################################################################################
## State

error_list = []                     # Any errors that happen while reading files
pool       = None


################################################################################
## Functions

# Order results for spreadsheet import
def order_file_details(metadata, clip_index, formatted_path, path):
    ## !!
    # add details to the results
    # Change the order or insert items here. This will be the final order of results

    movie_name = os.path.basename(path)

    return [
        formatted_path[0],            # last 3 directories of path
        formatted_path[1],
        formatted_path[2],
        movie_name,                   # movie file name
        metadata['size'],             # file size (b,kb,mb, or gb)
        metadata['creation_date'],    # file creation data
        '',                           # blank for notes
        clip_index,                   # clip number
        metadata['duration'],         # duration (HH:MM:SS)
        metadata['width-height'],     # width and height
        path.replace("/Volumes", '')  # full path (minus "/Volumes")
    ]

# Process a directory of files. Most of the magic happens here
def process_directory(directory_path, files):
    clip = 1
    total_clips = len(files)
    results = []

    def process_file(file_tuple):
        filename = file_tuple[0]
        clip = file_tuple[1]
        # for each file
        path = os.path.join(directory_path, filename)

        # get details
        metadata = get_video_metadata(path)
        clip_str = "%i/%i" % (clip, total_clips)

        # Split the path into components
        formatted_path = directory_path.replace("/Volumes/", "").split("/")[-3:]

        while len(formatted_path) < 3:
            # ensure formatted path has at least 3 items
            formatted_path.insert(0, '')

        results.append(order_file_details(metadata, clip_str, formatted_path, path))

        # increment clip
        clip += 1
        return order_file_details(metadata, clip_str, formatted_path, path)

    pool = ThreadPool(thread_count)
    results = pool.map(process_file, list(map(
        lambda x, y: (x, y), files, range(1, len(files)+1)
    )))
    pool.close()
    pool.join()
    return results


# Output the final results as csv and print
def save_results(processed_results):
    # save csv
    with open(csv_file_name, 'w') as csvfile:
        csvwriter = csv.writer(csvfile,
                               delimiter='\t',
                               quotechar='|',
                               quoting=csv.QUOTE_MINIMAL)
        # for row in processed_results:
        csvwriter.writerows(processed_results)


# Search for files and save
def find_files(root_directory):
    processed_results = []
    empty_dirs = []
    root_directory = os.path.realpath(os.path.expanduser(root_directory))

    if not os.path.exists(root_directory):
        error_exit("Invalid path! %s" % root_directory)

    dir_files_map = recursive_walk(root_directory, use_threads)

    print("%i directories found, processing..." % len(dir_files_map))

    for directory, files in dir_files_map: #dir_files_map.items():
        if len(files):
            processed_results += process_directory(directory, files)
        else:
            empty_dirs += [directory]

    print("**** %i results ****\n" % len(processed_results))
    save_results(processed_results)
    # print the resulting file
    with open(csv_file_name, 'r') as csvfile:
        print(csvfile.read())

    print("Output saved to %s <3\n" % csv_file_name)

    # print directories without results
    if empty_dirs:
        print("**** %i empty directories ****" % len(empty_dirs))

    # print directories without results
    if error_list:
        print("**** %i errors ****" % len(error_list))
        print("\n--".join(error_list[:50]))


################################################################################
## Utilities

def filter_files(filenames):
    results = []
    for filename in filenames:
        # only pick files that match a filter
        if str(filename).lower().endswith(include_ext):
            results.append(filename)
    if len(results):
        return results


dir_count       = 1
match_count     = 0

# Walk through a directory structure recursively
def recursive_walk(base_dir, top_level=False, threaded=False):
    # matches is a mapping of directory name to a list of movies
    matches         = [] #OrderedDict()
    global match_count, dir_count
    for root, dirnames, filenames in os.walk(base_dir):
        results = filter_files(filenames)
        if results:
            matches.append((root, results))
            # matches[root] =  results
            match_count += len(results)
        # if top_level:
        #     directories = map(lambda x: os.path.join(root, x), dirnames)
        #     for directory in directories:
        #         matches += recursive_walk(directory, threaded=True)
        #     break

        if threaded or top_level:
            directories = map(lambda x: os.path.join(root, x), dirnames)

            try:
                matches = [matches] + pool.map(recursive_walk, directories)
                pool.close()
                pool.join()
            except (KeyboardInterrupt, SystemExit):
                pool.terminate()
                pool.join()
                raise KeyboardInterrupt

            # matches = list(filter(lambda x: len(matches), matches))
            matches = list(reduce(lambda x, y: x + y, matches))
            break

        dir_count += 1
        if dir_count % 1000 == 0:
            print("%i directories scanned, %i movies" % (dir_count, match_count))

    return matches


# Function to extract the metadata of the input video file using ffprobe (ffmpeg)
def get_video_metadata(path_to_video_file):
    results = defaultdict(str)

    file_stat = os.stat(path_to_video_file)
    results['size'] = get_human_readable_size(file_stat.st_size, 2)
    results['creation_date'] = time.ctime(file_stat.st_ctime)

    try:
        # Use ffprobe to get metadata
        cmd = "%s -v quiet -print_format json -show_streams" % ffprobe_path
        args = shlex.split(cmd)
        args.append(path_to_video_file)

        # run the ffprobe process, decode stdout into utf-8 & convert to JSON
        ffprobe_output = subprocess.check_output(args).decode('utf-8')
        ffprobe_output = json.loads(ffprobe_output)

    except Exception as e:
        # ffprobe error, probably an unsupported codec
        error_list.append("--Unable to read %s" % path_to_video_file + str(e))
        results['error'] = str(e)
        return results

    try:
        # uncomment to see all the metadata available (fps, bitrate, etc):
        # import pprint
        # pp = pprint.PrettyPrinter(indent=2)
        # pp.pprint(ffprobe_output)

        # Get size and duration
        duration = float(ffprobe_output['streams'][0]['duration'])
        rounded_duration = int(round(duration))

        results['width-height'] = "%sx%s" % (
            ffprobe_output['streams'][0]['width'],
            ffprobe_output['streams'][0]['height']
        )

        # Extract the time (from duration seconds)
        seconds = int(rounded_duration) % 60
        minutes = int(rounded_duration / 60.0) % 60
        hours   = int(rounded_duration / (60.0 * 60.0))

        results['duration'] = "%02i:%02i:%02i" % (
            hours,
            minutes,
            seconds
        )

        # print(results)
    except Exception as e:
        error_list.append("--Error, possible corrupt: %s - %s" % (path_to_video_file, str(e)))
        results['error'] = str(e)

    return results


# Convert bytes to human readable format
def get_human_readable_size(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffix_idx = 0
    while size > 1024:
        suffix_idx += 1  # increment the index of the suffix
        size = size / 1024.0  # apply the division
    if suffix_idx == 0:
        precision = 0
    return "%.*f %s" % (precision, size, suffixes[suffix_idx])


# Print error message and exit
def error_exit(msg):
    print(msg)
    exit(2)


# Test if a program exists (ffprobe, in our case)
def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


ffprobe_path = which("ffprobe")


################################################################################
## Entry point

# Run the thing
if __name__ == "__main__":
    if len(sys.argv) < 2 or not sys.argv[1]:
        # require at least 1 argument
        error_exit(
            "Please include the path to the directory to scan! Usage:\n"
            "  python %s /Path/to/directory/to/scan" % __file__)
        exit(2)

    if not ffprobe_path:
        # ffprobe isn't installed
        error_exit(
            "You'll need to install ffmpeg first. Homebrew package manager is the easiest way:\n\n"
            "  /usr/bin/ruby -e \"$(curl -fsSL "
            "https://raw.githubusercontent.com/Homebrew/install/master/install)\"\n"
            "  brew install ffmpeg --with-fdk-aac --with-ffplay --with-freetype --with-libass "
            "--with-libquvi --with-libvorbis --with-libvpx --with-opus --with-x265\n"
            "\nSee: https://trac.ffmpeg.org/wiki/CompilationGuide/MacOSX#ffmpegthroughHomebrew\n"
            "(make sure installing homebrew is ok with company policies)")

    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

    pool = ThreadPool(thread_count)
    signal.signal(signal.SIGINT, original_sigint_handler)

    # Do it!
    find_files(sys.argv[1])

    print("\nComplete.\n"
          "To paste into Google sheets:\n"
          "Cmd+Shift+V")

    # Copy to clipboard
    if copy_results:
        print("\nResults copied to clipboard")
        command = 'cat %s | pbcopy' % csv_file_name
        os.system(command)
    else:
        print("\nTo copy output to clipboard:\n"
          "cat %s | pbcopy\n\n" % csv_file_name)
