import argparse

from bronze.evn import main as ingest_evn
from bronze.hydro import main as ingest_hydro
from bronze.nsmo import main as ingest_nsmo
from bronze.open_meteo import main as ingest_open_meteo


INGESTIONS = {
    "evn": ingest_evn,
    "hydro": ingest_hydro,
    "open_meteo": ingest_open_meteo,
    "nsmo": ingest_nsmo,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=INGESTIONS)
    args = parser.parse_args()

    INGESTIONS[args.dataset]()


if __name__ == "__main__":
    main()