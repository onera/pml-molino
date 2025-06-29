import argparse
import molino.notebooks
import subprocess


def main():
    parser = argparse.ArgumentParser()

    args = parser.parse_args()
    notebooks_path = molino.notebooks.__path__[0]

    subprocess.call(
        ["voila", notebooks_path],
    )

if __name__ == '__main__':
    main()