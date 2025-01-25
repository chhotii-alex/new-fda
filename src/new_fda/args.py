from argparse import ArgumentParser

def configure_parser():
    parser = ArgumentParser(
        description="Extract historic lab results"
    )
    parser.add_argument(
        "--redo",
        action="store_true",
        help="delete previously stored data from local database and start over from scratch"
    )
    parser.add_argument(
        "--step",
        type=int,
        help="step of the process to start with",
        default=0
    )
    return parser.parse_args()
