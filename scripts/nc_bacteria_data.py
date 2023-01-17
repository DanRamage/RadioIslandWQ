import os
import time
import requests
import logging.config
import xlrd
from datetime import datetime
from pytz import timezone
from nc_sample_data import nc_wq_sample_data

logger = logging.getLogger()

def get_entero_dates(entero_data_file):
    dates = []
    with open(entero_data_file, "r") as entero_data_file:
        for row_ndx, row in enumerate(entero_data_file):
            columns = row.split(",")
            if row_ndx > 0:
                date_time = datetime.strptime(columns[1], "%Y-%m-%d %H:%M:%S")
                dates.append(date_time)
    return(dates)

def download_sample_data(destination_dir, start_year, end_year):
    data_filename = None
    # https://reports.ncdenr.org/BOE/OpenDocument/opendoc/CrystalReports/viewrpt.cwr?id=9491&apsuser=REC_U&apspassword=mf_pr0_b0&apsauthtype=secEnterprise&cmd=export&export_fmt=U2FXLS:4&prompt0=Date(2010,1,1)&prompt1=Date(2010,12,31)&prompt2=*&prompt3=*&prompt4=*&prompt5=Public
    url_template = "https://reports.ncdenr.org/BOE/OpenDocument/opendoc/CrystalReports/viewrpt.cwr?id=9491&apsuser=REC_U&apspassword=mf_pr0_b0&apsauthtype=secEnterprise&cmd=export&export_fmt=U2FXLS:4&prompt0=Date({start_year},1,1)&prompt1=Date({end_year},12,31)&prompt2=*&prompt3=*&prompt4=*&prompt5=Public"
    start_time = time.time()
    try:
        data_filename = os.path.join(destination_dir,
                                     "{start_year}-{end_year}_sampledata.xls".format(start_year=start_year,
                                                                                     end_year=end_year))
        with open(data_filename, "wb") as output_file:
            full_url = url_template.format(start_year=start_year, end_year=end_year)
            req = requests.get(full_url)
            if req.status_code == 200:
                for chunk in req.iter_content(chunk_size=1024):
                    output_file.write(chunk)
                logger.info("Saved sample data in %f seconds" % (time.time() - start_time))
            else:
                logger.info("ERROR: Unable to download data. Status Code: {status}".format(req.status_code))
                data_filename = None
    except (IOError, Exception) as e:
        logger.exception(e)
        data_filename = None

    return (data_filename)


def parse_excel_data(file, monitoring_sites, wq_data_collection):
    logger.info("Parsing file: %s" % (file))
    wb = xlrd.open_workbook(filename=file)
    sheet = wb.sheet_by_name('Sheet1')
    row_headers = []
    results_ndx = \
        station_ndx = \
        county_ndx = \
        site_id_ndx = \
        area_ndx = \
        entero_gm_ndx = \
        entero_ssm_ndx = \
        date_ndx = None

    est_tz = timezone('US/Eastern')
    utc_tz = timezone('UTC')
    current_excel_station = None
    current_site_station_ndx = None
    for row_ndx, data_row in enumerate(sheet.get_rows()):
        if row_ndx != 0:
            station_name = data_row[station_ndx].value.strip().lower()
            site_id = "{id}{site}".format(id=data_row[area_ndx].value.strip(), site=data_row[site_id_ndx].value.strip())
            try:
                matched = False
                if current_excel_station is None or current_excel_station != station_name:
                    for ndx, site_nfo in enumerate(monitoring_sites):
                        if site_id == site_nfo.name:
                            logger.info("Processing row: {} Site: {} Name: {}".format(row_ndx, site_id, station_name))
                            matched = True
                            station_name = site_id
                            # station_name = site_nfo.description
                            current_excel_station = station_name
                            current_site_station_ndx = ndx
                else:
                    matched = True
            except ValueError as e:
                pass
            else:
                if matched:
                    try:
                        wq_sample_rec = nc_wq_sample_data()
                        wq_sample_rec.site_id = site_nfo.description
                        wq_sample_rec.station = site_nfo.name
                        try:
                            date_val = xlrd.xldate.xldate_as_datetime(data_row[date_ndx].value, wb.datemode)
                        except Exception as e:
                            logger.exception(e)
                            break
                        wq_sample_rec.date_time = (est_tz.localize(date_val)).astimezone(utc_tz)
                        wq_sample_rec.entero_gm = data_row[entero_gm_ndx].value
                        wq_sample_rec.entero_ssm = data_row[entero_ssm_ndx].value
                        wq_sample_rec.entero_gm2 = data_row[entero_gm2_ndx].value
                        wq_sample_rec.entero_ssm_cfu = data_row[entero_ssm_cfu_ndx].value
                        if len(wq_sample_rec.entero_ssm):
                            wq_sample_rec.value = wq_sample_rec.entero_ssm
                        else:
                            wq_sample_rec.value = wq_sample_rec.entero_ssm_cfu
                        logger.info("Site: %s Date: %s SSM: %s GM: %s" % (wq_sample_rec.station,
                                                                          wq_sample_rec.date_time,
                                                                          wq_sample_rec.entero_ssm,
                                                                          wq_sample_rec.entero_gm))
                        wq_data_collection.append(wq_sample_rec)
                    except Exception as e:
                        logger.exception(e)
        else:
            """
            Date	Area	Site	County	Description	Run	Location	Tier	24-hr Precipitation	Salinity	Water Temp	Tide	Current	Wind	Entero MPN1	Entero MPN2	Entero MPN3	Entero SSM	Entero GM	"Entero
            CFU1"	Entero CFU2	Entero CFU3	Entero SSM CFU	Entero_GM2	# of Days	E.Coli MPN1	E.Coli MPN2	E.Coli MPN3	E.Coli SSM	E.Coli GM	# of Days	Fecal MPN1	Fecal MPN2	Fecal MPN3	Fecal SSM	Fecal GM	# of Days	E.Coli CFU1	E.Coli CFU2	E.Coli CFU3	E.Coli SSF	E.Coli GM2	Fecal CFU1	Fecal CFU2	Fecal CFU3	Fecal SSC	Fecal GM2      
            """
            for cell in data_row:
                row_headers.append(cell.value)
            # Save the indexes for quicker access
            station_ndx = row_headers.index('Description')
            date_ndx = row_headers.index('Date')
            entero_gm_ndx = row_headers.index('Entero GM')
            entero_ssm_ndx = row_headers.index('Entero SSM')
            entero_gm2_ndx = row_headers.index('Entero_GM2')
            entero_ssm_cfu_ndx = row_headers.index('Entero SSM CFU')
            county_ndx = row_headers.index('County')
            area_ndx = row_headers.index('Area')
            site_id_ndx = row_headers.index('Site')

    logger.debug("Processed: {rows}".format(rows=row_ndx))
    return
