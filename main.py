import os, shutil
import argparse
import time

from utils.utils import read_csv, Data
from utils.download import parallel_download

from pathlib import Path


if __name__ == "__main__":
    # Clean up temporary files in the root directory at the start of the program
    root_dir = Path(".")
    for temp_file in root_dir.glob("*TEMP_MPY*"):
        try:
            os.remove(temp_file)
        except Exception as e:
            print(f"Failed to remove temp file {temp_file}: {e}")
    remove_mkv_dir = Path("E:/sedDatasets/AudioSet/tmp")

    for file in remove_mkv_dir.glob("*.mkv"):
        try:
            os.remove(file)
        except Exception as e:
            print(f"Failed to remove temp file {file}: {e}")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        choices=["only_audio", "only_video", "both_separate", "video"],
        help="select the type of the data you are interested in.",
    )
    parser.add_argument(
        "-c",
        "--classes",
        nargs="+",
        type=str,
        help="list of classes to find in a given directory of audioset files",
    )
    parser.add_argument(
        "-b",
        "--blacklist",
        nargs="+",
        type=str,
        help="list of classes which will exclude a clip from being downloaded",
    )
    parser.add_argument(
        "-d",
        "--destination_dir",
        type=str,
        help="directory path to put downloaded files into",
    )
    parser.add_argument(
        "-fs",
        "--sample_rate",
        type=int,
        help="Sample rate of audio to download. Default 16kHz (only applicable in audio mode)",
    )
    parser.add_argument(
        "--label_file",
        type=str,
        help="Path to CSV file containing AudioSet labels for each class",
    )
    parser.add_argument(
        "--csv_dataset",
        type=str,
        help="Path to CSV file containing AudioSet in YouTube-id/timestamp form",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print out more information about the download process",
    )

    parser.set_defaults(
        mode="both_separate",
        classes=None,
        blacklist=None,
        destination_dir=Path("E:/sedDatasets/AudioSet/balance"),#"./AudioSet",
        fs=16000,
        label_file="./Data_list/labels.csv",
        csv_dataset="./Data_list/balanced_train_segments.csv",
        verbose=True,
    )

    args = parser.parse_args()

    # Load data
    AudioSet = Data(args.csv_dataset, args.label_file, args.classes, args.blacklist)

    # Creat destination folders
    if os.path.isdir(args.destination_dir) == False:
        os.mkdir(args.destination_dir)
        for folder in AudioSet.classes_name:
            os.mkdir(os.path.join(args.destination_dir, folder))

    parallel_download(AudioSet, args)