import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import optparse
import time
import traceback

import requests


def download_sample_data(destination_dir, start_year, end_year):
    # https://reports.ncdenr.org/BOE/OpenDocument/opendoc/CrystalReports/viewrpt.cwr?id=9491&apsuser=REC_U&apspassword=mf_pr0_b0&apsauthtype=secEnterprise&cmd=export&export_fmt=U2FXLS:4&prompt0=Date(2010,1,1)&prompt1=Date(2010,12,31)&prompt2=*&prompt3=*&prompt4=*&prompt5=Public
    url_template = "https://reports.ncdenr.org/BOE/OpenDocument/opendoc/CrystalReports/viewrpt.cwr?id=9491&apsuser=REC_U&apspassword=mf_pr0_b0&apsauthtype=secEnterprise&cmd=export&export_fmt=U2FXLS:4&prompt0=Date({start_year},1,1)&prompt1=Date({end_year},12,31)&prompt2=*&prompt3=*&prompt4=*&prompt5=Public"
    start_time = time.time()
    try:
        data_filename = os.path.join(
            destination_dir,
            "{start_year}-{end_year}_sampledata.xls".format(
                start_year=start_year, end_year=end_year
            ),
        )
        with open(data_filename, "wb") as output_file:
            full_url = url_template.format(start_year=start_year, end_year=end_year)
            req = requests.get(full_url)
            if req.status_code == 200:
                for chunk in req.iter_content(chunk_size=1024):
                    output_file.write(chunk)
                print("Saved sample data in %f seconds" % (time.time() - start_time))

            else:
                print(
                    "ERROR: Unable to download data. Status Code: {status}".format(
                        req.status_code
                    )
                )
    except (IOError, Exception) as e:
        traceback.print_exc()
    return


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "--DestinationDir",
        dest="destination_dir",
        help="Destination directory to save the sampling data.",
    )
    parser.add_option(
        "--StartYear",
        dest="start_year",
        type="int",
        help="Date to begin the data downloads",
    )
    parser.add_option(
        "--EndYear", dest="end_year", type="int", help="Date to end the data downloads"
    )

    (options, args) = parser.parse_args()

    for year in range(options.start_year, options.end_year, 1):
        print("Download data for: {year}".format(year=year))
        download_sample_data(options.destination_dir, year, year)

    return


if __name__ == "__main__":
    main()
