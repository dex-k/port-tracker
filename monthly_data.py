import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
from io import BytesIO
import numpy as np
import json
import os

# TODO
# - Add debug logger

DYN_DATA_URL = (
    "https://www.transport.nsw.gov.au/data-and-research/freight-data/port-of-newcastle"
)
STATIC_URL = "https://opendata.transport.nsw.gov.au/data/dataset/5da0e3b9-e46a-4aa3-96c9-2574d83fe6fb/resource/3c5c9d89-ce54-4f72-9550-4077b7540612/download/port-of-newcastle.xlsx"

# Get an up-to-date fake useragent
ua = UserAgent()


# Create a reusable function with default headers
def spoof_get(url, **kwargs):
    """
    Performs a GET request to the given URL with a random User-Agent header.

    Any additional keyword arguments are passed to requests.get.
    """
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", ua.random)
    return requests.get(url, headers=headers, **kwargs)


def get_url_dynamic():
    response = spoof_get(DYN_DATA_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Search for the "opendata.transport.nsw.gov.au" link in the HTML content
    # 'tod' is "Transport Open Data"
    tod_url = soup.find(
        "a",
        href=lambda href: href and "opendata.transport.nsw.gov.au" in href,
    )["href"]

    response = spoof_get(tod_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    dynamic_url = soup.find("a", class_="resource-url-analytics")["href"]

    return dynamic_url


def get_xlsx():
    dynamic_url = get_url_dynamic()

    # check to see if the status url has changed, out of curiousity.
    # usure yet if it changes each month (or sooner)
    if dynamic_url != STATIC_URL:
        print("NOTE: Saved static URL different to dynamic URL, using new URL")

    url = dynamic_url or STATIC_URL

    # print(url)

    response = spoof_get(url)
    response.raise_for_status()

    return response.content


class TODExcelWorkbook:
    SHEET_NAMES = ["Readme", "Notes&Methods", "Port of Newcastle"]

    def __init__(self, bytes):
        # Create the spreadsheet dataframe
        self.spreadsheet = pd.read_excel(
            BytesIO(bytes), sheet_name=self.SHEET_NAMES, header=None
        )

        self.subsheets = {
            name: self._build_subsheet(hdr["slice"])
            for name, hdr in self._get_headings().items()
        }

    def _get_headings(self):
        # Extract the data headings
        hdr = self.spreadsheet["Port of Newcastle"].iloc[2]
        hdr = hdr[hdr.notna()]
        idx = list(hdr.index)
        return {
            name: {"slice": slice_}
            for name, slice_ in zip(
                hdr.values,
                (slice(start, end) for start, end in zip(idx, idx[1:] + [None])),
            )
        }

    def _build_subsheet(self, slice_):
        df = self.spreadsheet["Port of Newcastle"]

        # Convert slice to concrete indices
        start = slice_.start or 0
        stop = slice_.stop or df.shape[1]  # If None, go to last column
        step = slice_.step or 1

        # Combine 0 and slice indices with np.r_
        cols = np.r_[0, start:stop:step]

        # Select rows and columns
        res = df.iloc[3:, cols]

        # Make the first row the column headers
        res.columns = res.iloc[0].values
        res.drop(res.index[0], inplace=True)

        # The "Year" column at the end is redundant
        if "Year" in res.columns:
            res.drop(columns=["Year"], inplace=True)

        # Rename the month column and set it as a datetime index
        if "Month" in res.columns:
            res.rename(columns={"Month": "Date"}, inplace=True)
            res["Date"] = pd.to_datetime(res["Date"], format="%Y-%m-%d %H:%M:%S")

        # Sort by index
        res.sort_index(inplace=True)

        return res


def save_json(xlsx, path="./data/monthly/"):
    # create the directory if it doesn't exist
    if not os.path.exists(path):
        os.makedirs(path)

    for ss in xlsx.subsheets:
        filename = ss.replace(" ", "_").lower() + ".json"
        print(f"Saving {filename}...")

        # data = xlsx.subsheets[ss].to_json(orient="records", date_format="iso")
        # json_str = f'{{"{ss}": {data}}}'
        # with open(path + filename, "w") as f:
        #     json.dump(json.loads(json_str), f, indent=4)

        wrapped = {
            ss: xlsx.subsheets[ss]
            .map(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)
            .to_dict(orient="records")
        }

        # Save pretty-printed JSON
        with open(path + filename, "w") as f:
            json.dump(wrapped, f, indent=4)


def main():
    save_json(TODExcelWorkbook(get_xlsx()))


if __name__ == "__main__":
    main()
