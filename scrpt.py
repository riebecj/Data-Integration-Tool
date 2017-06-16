#!/usr/bin/env python
from tkinter import messagebox, Button, Entry, Tk, StringVar, Listbox, MULTIPLE, Label, Menu, Frame, Toplevel, Message
from tkinter.filedialog import askopenfilenames, asksaveasfilename, askdirectory
from PyPDF2 import PdfFileWriter, PdfFileReader
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from io import BytesIO, StringIO
from multiprocessing import Pool
from tkinter import font
from tqdm import tqdm
import configparser as cp
import getpass
import shutil
import tempfile
import zipfile
import os
import sys
import socket
import psycopg2
import itertools
import csv
import datetime
import subprocess
import re
import threading
import time
import pythoncom
try:
    import win32com.client
    import pywintypes
except ImportError:
    dirpath = r'C:\Python351\Lib\site-packages\win32'
    destpath = r'C:\Python351\Lib\site-packages\win32\lib'
    for i in ['pythoncom35.dll', 'pywintypes35.dll']:
        shutil.copy(os.path.join(dirpath, i), os.path.join(destpath, i))
    import win32com.client
    import pywintypes


class ToolTip(Toplevel):
    def __init__(self, widget, msg=None, msgfunc=None, delay=.75, follow=True):
        self.widget = widget
        self.parent = self.widget.master
        Toplevel.__init__(self, self.parent, bg='black', padx=1, pady=1)
        self.withdraw()
        self.overrideredirect(True)
        self.msqVar = StringVar()
        if msg is None:
            self.msqVar.set('No messge provided.')
        else:
            self.msqVar.set(msg)
        self.msgfunc = msgfunc
        self.delay = delay
        self.follow = follow
        self.visible = 0
        self.lastMotion = 0
        Message(self, textvariable=self.msqVar, bg="#FFFFDD", aspect=100).grid()
        self.widget.bind('<Enter>', self.spawn, "+")
        self.widget.bind('<Leave>', self.hide, "+")
        self.widget.bind('<Enter>', self.move, "+")

    def spawn(self, event):
        if event:
            self.visible = 1
            self.after(int(self.delay * 1000), self.show)

    def move(self, event):
        self.lastMotion = time.time()
        if not self.follow:
            self.withdraw()
            self.visible = 1
        self.geometry('+%i+%i' % (event.x_root + 10, event.y_root + 10))
        try:
            self.msqVar.set(self.msgfunc())
        except TypeError:
            pass
        self.after(int(self.delay * 1000), self.show)

    def hide(self, event):
        if event:
            self.visible = 0
            self.withdraw()

    def show(self):
        if self.visible == 1 and time.time() - self.lastMotion > self.delay:
            self.visible = 2
        if self.visible == 2:
            self.deiconify()


class PDFgenerator(object):
    def __init__(self, mission, name, path):
        self.name = name
        self.path = os.path.dirname(os.path.abspath(path))
        self.mission = mission
        self.fields = {
            "Date": {'x': 110, 'y': 700, 'value': str(self.mission.date_long)},
            "Squadron": {'x': 135, 'y': 677, 'value': str(Config().get()['Default']['squadron'][:3] + '-' +
                                                          Config().get()['Default']['squadron'][3:])},
            "ACFT Number": {'x': 137, 'y': 629, 'value': str(self.mission.buno)},
            "Length": {'x': 172, 'y': 584, 'value': str("{0:.1f}".format(float(self.mission.length)))},
            "AOR": {'x': 182, 'y': 561, 'value': str(Config().get()['Default']['AOR'])},
            "AEFs": {'x': 108, 'y': 536, 'value': str(self.mission.total_aefs)}
        }
        self.write()

    def write(self):
        packet = BytesIO()
        can = canvas.Canvas(packet, pagesize=letter)
        for _, val in self.fields.items():
            can.drawString(val['x'], val['y'], val['value'])

        can.save()

        packet.seek(0)
        new_pdf = PdfFileReader(packet)
        existing_pdf = PdfFileReader(open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                       r'config/template.pdf'), 'rb'))
        output = PdfFileWriter()

        page = existing_pdf.getPage(0)
        page.mergePage(new_pdf.getPage(0))
        output.addPage(page)

        output_stream = open(os.path.join(self.path, '{}.pdf'.format(self.name)), 'wb')
        output.write(output_stream)
        output_stream.close()


class PostgresqlDatabase(object):
    # Used for creating Data table
    field_type_string = '''ID int, sequence_id int, mdb_id int, report_no int, manual_report_no int,
        exc_from_analysis_flag int,ff_code varchar, band_no int, utc_time_stamp_usec timestamp without time zone,
        aef_index int, beam_index int, emitter_quality int2, tds_visible_flag int, report_update_type int2,
        ra_report_flag int, indeterminate_flag int, unknown_flag int, hostile_flag int, friend_flag int,
        platform_type int, location_status int, azimuth_type int, location_type int, azimuth_true_bearing_deg float8,
        azimuth_tolerance float8, elevation_type int, elevation_angle float8, elevation_angle_tolerance float8,
        lat_deg float8, lon_deg float8, ellipse_smajor_nm float8, ellipse_sminor_nm float8,
        ellipse_orientation_deg float8, range_emitter_nm float8, complex_emitter_flag int,
        classification_best_fit_avail int, classification_status int, classification_type int,
        classification_candidate_count int, emitter_beam_count int, jammer_tag int, harm_tag int, save_flag int,
        saved_by_operator_flag int, frequency_type int, freq_modulation_type int, freq_mhz float8, freq_min_mhz float8,
        freq_max_mhz float8, fm_excursion int, pw_usec float8, intra_pulse_ph_reversal_count int,
        current_amplitude int2, max_amplitude float8, pri_type int, pri_fri_precision_pri float8,
        pri_fri_deviation_window float8, stagger_levels_count int2, scan_type varchar, scan_rate_valid_flag int,
        scan_rate float8, alt_level_valid_flag int, alt_Level float8, ba_lat_long_valid_flag int, ba_latitude float8,
        ba_longitude float8, ba_trueHeading_angle float8, TrueHeading_valid_flag int, true_airspeed_valid_flag int,
        ba_true_airspeed float8, true_aoa_valid_flag int, true_aoa float8, pri_1 float8, pri_2 float8, pri_3 float8,
        pri_4 float8, pri_5 float8, pri_6 float8, pri_7 float8, pri_8 float8, pri_9 float8, pri_10 float8,
        pri_11 float8, pri_12 float8, pri_13 float8, pri_14 float8, pri_15 float8, pri_16 float8, emitter_name1 varchar,
        em_md_bm1 varchar, emitter_name2 varchar, em_md_bm2 varchar, emitter_name3 varchar, em_md_bm3 varchar,
        emitter_name4 varchar, em_md_bm4 varchar, emitter_name5 varchar, em_md_bm5 varchar, emitter_name6 varchar,
        em_md_bm6 varchar, emitter_name7 varchar, em_md_bm7 varchar, emitter_name8 varchar, em_md_bm8 varchar,
        emitter_name9 varchar, em_md_bm9 varchar, emitter_name10 varchar, em_md_bm10 varchar, emitter_name11 varchar,
        em_md_bm11 varchar, emitter_name12 varchar, em_md_bm12 varchar, emitter_name13 varchar, em_md_bm13 varchar,
        emitter_name14 varchar, em_md_bm14 varchar, emitter_name15 varchar, em_md_bm15 varchar, emitter_name16 varchar,
        em_md_bm16 varchar, AEAInternalStatus varchar, pw_type int, msg_type varchar, selected_enhanced_geo_active int,
        achvd_enhncd_geo_threshold int, ACFT_Bureau_Num int, BearingDist varchar, Coll_ID varchar, ELNOT varchar'''

    # Used for inserting data into table
    field_input_string = '''ID, sequence_id, mdb_id, report_no, manual_report_no,
        exc_from_analysis_flag,ff_code, band_no, utc_time_stamp_usec,
        aef_index, beam_index, emitter_quality, tds_visible_flag, report_update_type,
        ra_report_flag, indeterminate_flag, unknown_flag, hostile_flag, friend_flag, platform_type,
        location_status, azimuth_type, location_type, azimuth_true_bearing_deg, azimuth_tolerance,
        elevation_type, elevation_angle, elevation_angle_tolerance, lat_deg, lon_deg,
        ellipse_smajor_nm, ellipse_sminor_nm, ellipse_orientation_deg, range_emitter_nm,
        complex_emitter_flag, classification_best_fit_avail, classification_status, classification_type,
        classification_candidate_count, emitter_beam_count, jammer_tag, harm_tag, save_flag,
        saved_by_operator_flag, frequency_type, freq_modulation_type, freq_mhz, freq_min_mhz,
        freq_max_mhz, fm_excursion, pw_usec, intra_pulse_ph_reversal_count, current_amplitude,
        max_amplitude, pri_type, pri_fri_precision_pri, pri_fri_deviation_window,
        stagger_levels_count, scan_type, scan_rate_valid_flag, scan_rate,
        alt_level_valid_flag, alt_Level, ba_lat_long_valid_flag, ba_latitude, ba_longitude,
        ba_trueHeading_angle, TrueHeading_valid_flag, true_airspeed_valid_flag, ba_true_airspeed,
        true_aoa_valid_flag, true_aoa, pri_1, pri_2, pri_3, pri_4,
        pri_5, pri_6, pri_7, pri_8, pri_9, pri_10, pri_11,
        pri_12, pri_13, pri_14, pri_15, pri_16, emitter_name1,
        em_md_bm1, emitter_name2, em_md_bm2, emitter_name3, em_md_bm3,
        emitter_name4, em_md_bm4, emitter_name5, em_md_bm5, emitter_name6,
        em_md_bm6, emitter_name7, em_md_bm7, emitter_name8, em_md_bm8,
        emitter_name9, em_md_bm9, emitter_name10, em_md_bm10, emitter_name11,
        em_md_bm11, emitter_name12, em_md_bm12, emitter_name13, em_md_bm13,
        emitter_name14, em_md_bm14, emitter_name15, em_md_bm15, emitter_name16,
        em_md_bm16, AEAInternalStatus, pw_type, msg_type, selected_enhanced_geo_active,
        achvd_enhncd_geo_threshold, ACFT_Bureau_Num, BearingDist, Coll_ID, ELNOT'''

    # Used for creating EOB table
    eob_type_string = '''emitter_key varchar, country_code varchar, Allegiance varchar, latitude float8,
    longitude float8, equip_code varchar, midb_name text, be_number varchar, fac_unit_name text,
    mod_date timestamp without time zone'''

    # Used for inserting data into EOB table
    eob_field_string = '''emitter_key, country_code, Allegiance, latitude, longitude, equip_code, midb_name, be_number,
    fac_unit_name, mod_date'''

    # Used for creating elnot table
    elnot_type_string = '''emitter_key varchar, elnot varchar'''

    # Used for inserting elnot data
    elnot_input_string = '''emitter_key, elnot'''

    def __init__(self, tablename):
        super(PostgresqlDatabase, self).__init__()
        self.connected = False
        self.table_name = tablename
        self.validate_network()
        if self.connected:
            self.server = Config().get()['Default']['server']
            self.initial_pass = True
            self.conn, self.cur = self.connection()

    def validate_network(self):
        try:
            host = socket.gethostbyaddr(Config().get()['Default']['server'])
            s = socket.create_connection((host[0], 5432), 2)
            s.close()
            self.connected = True
        except (socket.gaierror, socket.herror, ConnectionRefusedError) as e:
            print(e)
            Handler().info('Connection Fail', "Cannot Connect to '{}'.\nEnter Proper IP or Domain Name.".format(
                Config().get()['Default']['server']))

            def callback():
                if connection(ip_domain_entry.get()):
                    Config().save('Default', 'server', ip_domain_entry.get())
                    Config().update_server()
                    self.connected = True
                    form.destroy()
                else:
                    messagebox.showerror('Connection Fail',
                                         "IP/Domain not connected to the network. Verify connectivity and try again")
                    form.focus_force()

            def connection(name):
                try:
                    name = socket.gethostbyaddr(name)
                    s2 = socket.create_connection((name[0], 5432), 2)
                    s2.close()
                    return True
                except socket.gaierror:
                    return False

            form = Tk()
            form.title("Connection Setup")
            ip_domain_entry = Entry(form, width=35)
            ip_domain_entry.pack()
            ip_domain_entry.focus_set()
            check = Button(form, text="Submit", width=10, command=callback)
            check.pack()
            form.focus_force()
            form.mainloop()
        except socket.timeout:
            Handler().show_error('Connection Timeout', "Connection to server '{}' timed out. "
                                 "Ensure system is connected and online.".format(Config().get()['Default']['server']))
            self.connected = False
            return

    def connection(self):
        if self.validate_connection():
            conn = psycopg2.connect(
                "dbname='aea' user='gale' host='{}' password='GALEdatabase01!!'".format(self.server))
            cur = conn.cursor()

            if self.validate_table(conn, cur):
                return conn, cur
            else:
                self.create_table(conn, cur)
                return conn, cur

        else:
            self.create_database()
            conn = psycopg2.connect(
                "dbname='aea' user='gale' host='{}' password='GALEdatabase01!!'".format(self.server))
            cur = conn.cursor()
            self.create_table(conn, cur)
            return conn, cur

    def upload(self, data):
        if 'aef' not in self.table_name:
            if self.initial_pass:
                self.cur.execute('truncate {};'.format(self.table_name))
                self.conn.commit()
                self.initial_pass = False
        if 'aef' in self.table_name:
            string = self.field_input_string
        elif 'eob' in self.table_name:
            string = self.eob_field_string
        else:
            string = self.elnot_input_string

        mogrify = "(" + ','.join(["%s"] * len(string.split(','))) + ")"

        if 'aef' in self.table_name:
            if 'ID' in data[0][0]:
                del data[0]
            index_to_del = []
            for index, line in enumerate(data):
                if line[64] is None or line[64] == '':
                    index_to_del.append(index)
            for index in reversed(index_to_del):
                del data[index]
        elif 'eob' in self.table_name:
            for index, j in enumerate(data):
                if isinstance(j, str):
                    data[index] = j.replace("'", "")
        try:
            data = tuple(tuple(x) for x in data)
            args_str = b','.join(self.cur.mogrify(mogrify, x) for x in data).decode()
            self.cur.execute("INSERT INTO {} ({}) VALUES ".format(self.table_name, string) + args_str)
            self.conn.commit()
        except IndexError:
            for row in data:
                print(len(string.split(',')), len(row))
                print(row)
                row = tuple(v if v is not None else '' for v in row)
                self.cur.execute("INSERT INTO {} ({}) VALUES {};".format(self.table_name, string, str(row)))
            self.conn.commit()

    def get(self, string):
        self.cur.execute(string)
        return self.cur.fetchall()

    def delete_duplicates(self):
        self.cur.execute("DELETE FROM aea_aef_data_table "
                         "WHERE Key IN (SELECT Key "
                         "FROM (SELECT Key, "
                         "ROW_NUMBER() OVER (partition BY ID, utc_time_stamp_usec, report_no, freq_mhz, pw_usec "
                         "ORDER BY ID) AS rnum "
                         "FROM aea_aef_data_table) t "
                         "WHERE t.rnum > 1);")
        self.conn.commit()

    def validate_connection(self):
        try:
            conn = psycopg2.connect(
                "dbname='aea' user='gale' host='{}' password='GALEdatabase01!!'".format(self.server))
            conn.close()
            return True

        except psycopg2.OperationalError:
            return False

    def validate_table(self, conn, cur):
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{}';".format(self.table_name))

        if cur.fetchone()[0] != 1:
            conn.commit()
            return False
        else:
            return True

    def create_database(self):
        conn = psycopg2.connect(
            "dbname='postgres' user='gale' host='{}' password='GALEdatabase01!!'".format(self.server))
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE aea")
        cur.close()
        conn.close()

    def create_table(self, conn, cur):
        if 'aef' in self.table_name:
            string = self.field_type_string
        elif 'eob' in self.table_name:
            string = self.eob_type_string
        else:
            string = self.elnot_type_string

        cur.execute('CREATE TABLE {} (Key serial PRIMARY KEY, {});'.format(self.table_name, string))
        conn.commit()
        conn.close()

    def exit(self):
        self.exit()
        self.conn.close()


class Config(object):
    files = {
        'AEA_DB.connection': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <gale:DatabaseSpecification xmlns:gale="urn:com:ngc:gale:infrastructure:configuration:specification:generated" xmlns:ns2="urn:com:ngc:gale:infrastructure:configuration:specification:generated:data" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" gale:Name="AEA_DB" gale:Vendor="PostGres" xsi:schemaLocation="urn:com:ngc:gale:infrastructure:configuration:specification:generated file:///C:/Program%20Files%20(x86)/GALE/config/specification/Schema/DatabaseSpecification.xsd">
        <gale:Comment>RDBMS connection information for a local postgres server</gale:Comment>
        <gale:Driver>org.postgresql.Driver</gale:Driver>
        <gale:Url>jdbc:postgresql://{}:5432/aea</gale:Url>
        <gale:Provider>System.Data.Odbc</gale:Provider>
        <gale:SecureCommunication>false</gale:SecureCommunication>
        <gale:Property gale:Name="DataDefinitionConfiguration">AEA_DB</gale:Property>
        <gale:Version>-1</gale:Version>
        <gale:UseParallelHint>false</gale:UseParallelHint>
        <gale:NumberOfDatabaseQueryThreadGroups>1</gale:NumberOfDatabaseQueryThreadGroups>
        <gale:MaxConnectionPoolSize>50</gale:MaxConnectionPoolSize>
    </gale:DatabaseSpecification>""",
        'AEA_EOB.connection': """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <gale:DatabaseSpecification xmlns:gale="urn:com:ngc:gale:infrastructure:configuration:specification:generated" xmlns:ns2="urn:com:ngc:gale:infrastructure:configuration:specification:generated:data" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" gale:Name="AEA_EOB" gale:Vendor="PostGres" xsi:schemaLocation="urn:com:ngc:gale:infrastructure:configuration:specification:generated file:///C:/Program%20Files%20(x86)/GALE/config/specification/Schema/DatabaseSpecification.xsd">
        <gale:Comment>RDBMS connection information for a local postgres server</gale:Comment>
        <gale:Driver>org.postgresql.Driver</gale:Driver>
        <gale:Url>jdbc:postgresql://{}:5432/aea</gale:Url>
        <gale:Provider>System.Data.Odbc</gale:Provider>
        <gale:SecureCommunication>false</gale:SecureCommunication>
        <gale:Property gale:Name="DataDefinitionConfiguration">AEA_EOB</gale:Property>
        <gale:Version>-1</gale:Version>
        <gale:UseParallelHint>false</gale:UseParallelHint>
        <gale:NumberOfDatabaseQueryThreadGroups>1</gale:NumberOfDatabaseQueryThreadGroups>
        <gale:MaxConnectionPoolSize>50</gale:MaxConnectionPoolSize>
    </gale:DatabaseSpecification>""",
        'AEA.connection': """<?xml version="1.0" encoding="UTF-8"?>
    <!-- ************************** CLASSIFICATION ******************************
    *                                                                          *
    *                               SECRET                               *
    *                                                                          *
    ************************************************************************ -->
    <!-- **************************** COPYRIGHT *********************************
    # Copyright (c) (2015-2016) by the U.S. Defense Intelligence Agency (DIA)
    #
    # The Department of Defense has unlimited rights in this computer software and
    # it may be reproduced by and for the Department of Defense has pursuant to the
    # copyright license under the clause at DFARS 252.227.7013 (October 1988).
    ************************************************************************ -->
    <!-- ***************************** PREFACE **********************************
    *
    * File Name: PostgresExample.connection.xml
    *
    * Purpose: PostGres rdbms connection information
    *
    * Revision History:
    *
    * Name           Date         DR      Description
    * Blakely        Jan, 09 2007         initial creation
    * jcasler	 Feb, 16 2009	WI5705	Removed DSN information
    * zberk          Feb 24, 2014      WI28321    Changed classification/header info.
    * nxavier	  July 18. 2014   WI29094  Updated version number to be set to the
     *															current GALE version (5.3.1)
    * nxavier    Aug 19, 2014   WI29408  Changed default port for the WS to 8413
     * nxavier    Sep  9, 2014    WI29568  Changed default port for the WS to 443
     * zberk		Dec 22, 2014	WI30276	Updated version number.
     * gfox  	  Jun 12, 2015   WI31457 Updated version number to be set to the
     *															current GALE version (5.3.2)
     * MYSmith  22 APR 2016  GALE-387  Update version number to 5.4.0.0
     * BKane    20 Dec 2016  GALE-334  Update version # and fix implied tags
    * Notes:
    *
    ************************************************************************ -->
    <gale:DatabaseSpecification xmlns:gale="urn:com:ngc:gale:infrastructure:configuration:specification:generated" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:com:ngc:gale:infrastructure:configuration:specification:generated ../Schema/DatabaseSpecification.xsd" gale:Name="AEA"gale:Vendor="PostGres">
        <gale:Comment>RDBMS connection information for a local postgres server</gale:Comment>
        <gale:Driver>org.postgresql.Driver</gale:Driver>
        <gale:Url>jdbc:postgresql://{}:5432/aea</gale:Url>
        <gale:Provider>System.Data.Odbc</gale:Provider>
        <gale:SecureCommunication>false</gale:SecureCommunication>
        <gale:Property gale:Name="DataDefinitionConfiguration">MEPDatabase</gale:Property>
        <gale:Version>5.4.2.0</gale:Version>
    <gale:UseParallelHint>false</gale:UseParallelHint>
    <gale:NumberOfDatabaseQueryThreadGroups>1</gale:NumberOfDatabaseQueryThreadGroups>
    <gale:MaxConnectionPoolSize>50</gale:MaxConnectionPoolSize>
    </gale:DatabaseSpecification>"""
    }

    def __init__(self):
        self.config = cp.ConfigParser()
        self.load()

    def save(self, section, item, val):
        self.config[section][item] = val

        with open('C:\EWDS-DIA\config\ADIT.ini', 'w') as configfile:
            self.config.write(configfile)

    def load(self):
        self.config.read('C:\EWDS-DIA\config\ADIT.ini')

    def get(self):
        return self.config

    def write_files(self, filename, dir_, data):
        if os.path.isfile(os.path.join(dir_, filename)):
            for retry in range(100):
                try:
                    os.remove(os.path.join(dir_, filename))
                    break
                except PermissionError:
                    pass
        with open(os.path.join(dir_, filename), 'w+') as f:
            f.write(data)

    def update_server(self):
        conn = self.get()['Default']['server']
        for k, v in Config.files.items():
            if k.split(".")[0] == 'AEA':
                a = k + ".xml"
                b = r"C:\Program Files (x86)\GALE\config\specification\DatabaseConnection"
                c = r"D:\GALE\config\specification\DatabaseConnection"
                self.write_files(a, b if os.path.isdir(b) else c, v.format(conn))
            elif k.split(".")[0] == 'AEA_DB':
                a = k + ".xml"
                b = r"C:\Users\{}\AppData\Local\GALE_Application\config\specification\DatabaseConnection".format(
                    str(getpass.getuser()))
                c = r"C:\GALEData\config\specification\DatabaseConnection"
                self.write_files(a, b if os.path.isdir(b) else c, v.format(conn))
            else:
                a = k + ".xml"
                b = r"C:\Users\{}\AppData\Local\GALE_Application\config\specification\DatabaseConnection".format(
                    getpass.getuser())
                c = r"C:\GALEData\config\specification\DatabaseConnection"
                self.write_files(a, b if os.path.isdir(b) else c, v.format(conn))


class Handler(object):
    def __init__(self):
        pass

    @staticmethod
    def show_error(title, text):
        window = Tk()
        window.withdraw()
        messagebox.showerror(title, text)
        window.destroy()

    @staticmethod
    def yes_no(title, text):
        window = Tk()
        window.withdraw()
        result = messagebox.askyesno(title, text)
        window.destroy()

        return result

    @staticmethod
    def info(title, text):
        window = Tk()
        window.withdraw()
        messagebox.showinfo(title, text)
        window.destroy()


class FileDialogs(object):
    def __init__(self):
        pass

    def open(self, options=None):
        """File Dialog that allows for the selection, and proper verification, of importable data files."""
        while True:
            if options is None:
                options = {'filetypes': [('Mission Files', '.apf .mdb .accdb .zip .csv'), ('All Files', '*.*')]}
            path = Config().get()["Default"]['filesdirectory']

            window = Tk()
            window.withdraw()
            file = askopenfilenames(parent=window, initialdir=path, **options)
            window.destroy()

            if file == '':
                return

            elif len(file) > 8:
                Handler.show_error("File Memory Error!", "Cannot Select More Than 8 Files at a Time.")

            else:
                if (any(['.mdb' in x for x in file]) or any(['.accdb' in y for y in file])) and all(
                        ['EWDS' and 'Lite' in z for z in file]) and len(file) > 1:
                    Handler.show_error("EOB Error!", "Cannot Select More Than 1 EOB Files at a Time.")

                elif self.check_types(file):
                    Config().save('Default', 'filesdirectory', os.path.dirname(os.path.commonprefix(file)))
                    break
                else:
                    Handler.show_error("Type Error!", "ALL FILES MUST BE THE SAME FILE TYPE!")

        return file

    def save(self, template):
        """File Dialog that allows the user to save files with specified settings."""

        window = Tk()
        window.withdraw()
        file = asksaveasfilename(parent=window, defaultextension='.zip', filetypes=[('Zip file', '.zip')],
                                 initialfile=template)
        window.destroy()

        return file

    def directory(self, update=True):
        window = Tk()
        window.withdraw()
        dirname = askdirectory(title="Select Where to Put Database Dump File",
                               initialdir=r"C:\Users\{}\Desktop".format(getpass.getuser()))
        window.destroy()

        if update:
            Config().save("Default", "DatabaseDirectory", dirname)
            Config().save("Default", "UserModified", "Yes")
        return dirname

    def check_types(self, files):
        """Validates that all imported files are of the same file type."""
        ftype = files[0].split('.')[-1]

        for file in files:
            if file.endswith('.{}'.format(ftype)):
                continue
            else:
                return False

        return True


class TempDir(object):
    def __init__(self):
        pass

    def make(self):
        return tempfile.mkdtemp()

    def remove(self, tempdir):
        shutil.rmtree(tempdir)


class Data(object):
    """
    Data is a container class for VAQ/VMAQ post-flight ESM mission data.

    Input:
    -----------
    Data(path):         where path is the path to the mission file for import. Can be (.accdb, .mdb, .csv) or a (.zip)
                        containing one of the pevious three.

    Methods:
    -----------
    file_attributes:    Gets the attributes for the mission file. Will also unzip to temp directory if file is of
                        mimetype (.zip). Then sends file attributes to the proper import function.
                            filename    = name of file
                            path        = path to file
                            mimetype    = type of file (.zip, .mdb, .accdb, .csv)

    import_csv:         Imports mission data from (.csv) file. Applies proper formatting, if needed, and assigns it to
                        Data.data

    import_access:      Imports mission data from (.accdb, .mdb) file. Applies proper formatting, if needed, and assigns
                        it to Data.data

    get_data_attributes:    Gets or generates important data attributes once the data has been imported from file.
                                start_time  = mission start time in 00 hrs
                                stop_time   = mission stop time in 00 hrs (rounded up at 30min)
                                date        = mission date in YYMMDD digits
                                date_long   = mission date in MM/DD/YY
                                length      = mission length in 0.0 hrs
                                total_aefs  = total number of unique AEFs (systems) in mission

    generate_elnot:     Generates elnot from available data source (PostgreSQL Database). If elnot not known, and system
                        is COMNAV, generates equitable PEN. If unable to fit into PEN critera and no elnot available, it
                        is classified as an L-zip.

    rount_time:         Static. Used to round the stop_time attribute.
    """

    def __init__(self, path):
        self.valid = True
        self.tempfile = ''
        self.map = {}
        self.unmapped = []
        self.count = 0
        self.headers = []
        self.start_time = ''
        self.stop_time = ''
        self.date_changed = False
        self.previous = ''
        self.size = 0
        self.buno = 0
        self.date_for_format = ''
        self.date = ''
        self.date_long = ''
        self.length = 0
        self.total_aefs = 0
        self.mimetype = path.split('.')[-1]
        self.filename = path.split('/')[-1].split('.')[0]
        self.path = os.path.join(path.split('/')[0] + '\\', *path.split('/')[1:-1])
        self.file_attributes()
        self.construct_lookup()

    def construct_lookup(self):
        db = PostgresqlDatabase('emitter_elnot')
        if not db.connected:
            self.valid = False
            return
        table = [next(iter(b)) for b in db.get("SELECT emitter_key from emitter_elnot")]
        keys = []
        for x in table:
            if x not in keys:
                keys.append(x)

        mapping = []
        for key in keys:
            elnots = [next(iter(v)) for v in
                      db.get("SELECT elnot from emitter_elnot WHERE emitter_key='{}'".format(key))]
            if len(elnots) == 1:
                mapping.append((key, elnots[0]))
            elif sum([elnots[0][:4] in x for x in elnots]) == 1 and len([elnots[0][:4] in x for x in elnots]) == 2:
                mapping.append((key, elnots[0][:4] + 'Y'))
            elif sum([elnots[1][:4] in x for x in elnots]) > 1:
                mapping.append((key, elnots[1][:4] + 'Y'))
            else:
                mapping.append((key, 'N407A'))

        for reference in mapping:
            self.map[reference[0]] = reference[1]

    def read(self, buffer):
        pool = Pool(4)
        if self.mimetype == 'csv':
            with open(os.path.join(
                    self.tempfile if self.tempfile != '' else self.path, self.filename + '.csv'), 'r') as inp:
                while True:
                    lines = pool.map(self.format_data, inp.readlines(buffer))
                    if not lines:
                        break
                    yield lines
        else:
            pythoncom.CoInitialize()
            conn = win32com.client.Dispatch(r'ADODB.Connection')
            conn.Open('PROVIDER = Microsoft.ACE.OLEDB.12.0;DATA SOURCE = ' +
                      os.path.join(self.tempfile if self.tempfile != '' else self.path, '.'.join([self.filename,
                                                                                                  self.mimetype]) + ';')
                      )
            rs = win32com.client.Dispatch(r'ADODB.Recordset')
            rs.Open("SELECT * FROM aea_aef_report_data", conn, 1, 3)
            initial = True
            while not rs.EOF:
                lines = pool.map(self.format_data, [line for line in zip(*rs.GetRows(100, 0))])
                if initial:
                    if not any(["selected_enhanced" in x for x in self.headers]):
                        headers = self.headers.copy()
                        headers += ['"selected_enhanced_geo_active"', '"achvd_enhncd_geo_threshold"',
                                    '"ACFT_Bureau_Num"', '"BearingDist"', '"Coll_ID"', '"ELNOT"']
                    else:
                        headers = self.headers.copy()
                        headers += ['"ACFT_Bureau_Num"', '"BearingDist"', '"Coll_ID"', '"ELNOT"']
                    lines.insert(0, headers)
                yield lines

    def format_data(self, row):
        if isinstance(row, str):
            row = row.split(',')
            if 'ID' in row[0]:
                row = [x for x in row if len(x) > 1]
                if not any(["selected_enhanced" in x for x in self.headers]):
                    row += ['"selected_enhanced_geo_active"', '"achvd_enhncd_geo_threshold"', '"ACFT_Bureau_Num"',
                            '"BearingDist"', '"Coll_ID"', '"ELNOT"']
                    return row

                if not any(["ACFT_Bureau_Num" in x for x in self.headers]):
                    row += ['"ACFT_Bureau_Num"', '"BearingDist"', '"Coll_ID"', '"ELNOT"']
                    return row

                if not any(['BearingDist' in x for x in self.headers]):
                    row += ['"BearingDist"', '"Coll_ID"', '"ELNOT"']
                    return row

                if not any(['ELNOT' in x for x in self.headers]):
                    row.append('"ELNOT"')
                    return row

                return [x.rstrip() for x in row]

            else:
                if not any(["selected_enhanced" in x for x in self.headers]):
                    row += [0, 0, self.buno, '', 'EA18G' if self.buno != 999999 else "EA6B", self.generate_elnot(row)]
                    return row

                if not any(['ACFT_Bureau_Num' in x for x in self.headers]):
                    row += [self.buno, '', 'EA18G' if self.buno != 999999 else "EA6B", self.generate_elnot(row)]
                    return row

                if not any(['BearingDist' in x for x in self.headers]):
                    row += ['', 'EA18G' if self.buno != 999999 else "EA6B", self.generate_elnot(row)]
                    return [x.rstrip() for x in row]

                if not any(['ELNOT' in x for x in self.headers]):
                    row.append(self.generate_elnot(row))
                    return [x.rstrip() for x in row]

                return [x.rstrip() for x in row]
        else:
            row = list(row)
            _datetime = (self.date_for_format + datetime.timedelta(microseconds=int(row[8])))
            if self.previous == '':
                self.previous = _datetime
            else:
                if _datetime <= self.previous:
                    self.date_changed = True

            if self.date_changed:
                _datetime += datetime.timedelta(days=1)

            row[8] = _datetime.strftime('%Y-%m-%d %H:%M:%S')

            if not any(["selected_enhanced" in x for x in self.headers]):
                row += [0, 0]

            row += [self.buno, '', "EA18G" if not int(self.buno) == 999999 else "EA6B", self.generate_elnot(row)]

            row = [x.rstrip() if isinstance(x, str) else x for x in row]
            return row

    def file_attributes(self):
        if self.mimetype == 'zip':
            self.tempfile = TempDir().make()
            with zipfile.ZipFile(os.path.join(self.path, self.filename + '.zip'), "r") as zipx:
                zipx.extractall(self.tempfile)

            for file in os.listdir(self.tempfile):
                self.filename, self.mimetype = file.split('.')
        if self.mimetype == 'csv':
            with open(os.path.join(
                    self.tempfile if self.tempfile != '' else self.path, self.filename + '.csv'), 'rt') as f:
                reader = csv.reader(f)
                lines = list(reader)
                self.count = len(lines)
                if not self.count > 1:
                    self.valid = False
                    Handler.show_error('File Error', 'File {} contains no valid data.'.format(self.filename))
                    return
                self.buno = [x for x in lines[1][-5:] if re.match(r"\d{6}", x)][0] if len(lines[1]) >= 126 else 999999
                self.headers = lines[0]
                self.start_time = int(lines[1][8].split(':')[0].split(' ')[-1])
                self.stop_time = self.round_time(lines[-1][8])
                self.size = os.path.getsize(os.path.join(self.tempfile if self.tempfile != '' else
                                                         self.path, self.filename + '.' + self.mimetype))
                self.date = datetime.datetime.strptime(lines[1][8], "%m/%d/%Y %H:%M" if '/' in lines[1][8] else
                                                       "%Y-%m-%d %H:%M:%S").strftime("%y%m%d")
                self.date_long = datetime.datetime.strptime(lines[1][8], "%m/%d/%Y %H:%M" if '/' in lines[1][8] else
                                                            "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%y")
                self.length = (datetime.datetime.strptime(lines[-1][8], "%m/%d/%Y %H:%M" if '/' in lines[-1][8] else
                               "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(lines[1][8], "%m/%d/%Y %H:%M" if '/'
                               in lines[1][8] else "%Y-%m-%d %H:%M:%S")).total_seconds() / (60.0 * 60.0)
                aefs = []
                for row in lines:
                    if not row[3] in aefs:
                        aefs.append(row[3])
                self.total_aefs = len(aefs)
        elif any(self.mimetype in x for x in ['accdb', 'mdb', 'MDB', 'ACCDB', 'apf']):
            conn = win32com.client.Dispatch(r'ADODB.Connection')
            try:
                conn.Open('PROVIDER = Microsoft.ACE.OLEDB.12.0;DATA SOURCE = ' +
                          os.path.join(self.tempfile if self.tempfile != '' else self.path, '.'.join(
                              [self.filename, self.mimetype]) + ';'))
            except pywintypes.com_error:
                Handler().show_error('COM_Error', '{} is in an unrecognized database format.'.format('.'.join(
                    [self.filename, self.mimetype])))
                self.valid = False
                return
            rs = win32com.client.Dispatch(r'ADODB.Recordset')
            rs.Open("SELECT * FROM aea_aef_report_data", conn, 1, 3)

            while not rs.EOF:
                self.count += 1
                rs.MoveNext()
            rs.MoveFirst()

            if not self.count > 1:
                self.valid = False
                Handler.show_error('File Error',
                                   'File {} contains no valid data.'.format(self.filename))
                return

            first_row = [x.rstrip() if isinstance(x, str) else x for x in [next(iter(x)) for x in rs.GetRows(1, 0)]]
            rs.MoveLast()
            last_row = [x.rstrip() if isinstance(x, str) else x for x in [next(iter(x)) for x in rs.GetRows(1, 0)]]
            rs.MoveFirst()

            for x in range(rs.Fields.Count):
                self.headers.append('"' + rs.Fields.Item(x).Name + '"')

            self.start_time = str(datetime.timedelta(microseconds=int(first_row[8]))).split(':')[0]
            self.stop_time = self.round_time(str(datetime.timedelta(microseconds=int(last_row[8]))))

            self.size = self.count
            aefs = []
            for x in range(self.count):
                row = [next(iter(x)) for x in rs.GetRows(1, 0)]
                if not row[3] in aefs:
                    aefs.append(row[3])

            self.total_aefs = len(aefs)
            rs.Close()
            try:
                rs.Open("SELECT * FROM eau_dx_archive_file_header", conn, 1, 3)
                self.buno = [str(next(iter(x))) for x in rs.GetRows(1, 0)][5]
                rs.Close()
            except pywintypes.com_error:
                self.buno = 999999
            date = ''
            try:
                rs.Open("SELECT * FROM utc_month_day_ext", conn, 1, 3)
                data = [str(next(iter(x))) for x in rs.GetRows(1, 0)]
                row = [data[2]] + data[4:]
                date = datetime.datetime.strptime(" ".join(row), '%y %m %d')
                rs.Close()
            except pywintypes.com_error:
                date_from_file = self.filename.split('_')[0]
                if not date_from_file.isdigit():
                    for c in [x for x in itertools.combinations(["%Y", '%y', '%b', '%m'], 2) if
                              any(t in x for t in ["%Y", '%y']) and any(t in x for t in ['%b', '%d'])]:
                            try:
                                date = datetime.datetime.strptime(" ".join([x for x in re.split('(\d+)', date_from_file)
                                                                            if not x == '']), '{0} {1} %d'.format(*c))
                            except ValueError:
                                pass
                else:
                    if len(str(date_from_file)) > 6:
                        date = datetime.datetime.strptime(date_from_file, "%Y%m%d")
                    else:
                        date = datetime.datetime.strptime(date_from_file, "%y%m%d")
            self.date_for_format = date
            self.date = date.strftime("%y%m%d")
            self.date_long = date.strftime("%m/%d/%y")

            self.length = (((date if last_row[8] > first_row[8] else date + datetime.timedelta(days=1)) +
                            datetime.timedelta(microseconds=int(last_row[8])) - (date +
                                                                                 datetime.timedelta(microseconds=int(
                                                                                     first_row[
                                                                                         8])))).total_seconds() / (
                           60.0 * 60.0))

            conn.Close()
        else:
            Handler.show_error('Type Error', 'File of type {} not a valid mission data file.'.format(self.mimetype))
            self.valid = False
            if self.tempfile != '':
                TempDir().remove(self.tempfile)

    def generate_elnot(self, row):
        ranges = {1: (240, 940),
                  2: (940, 1172),
                  3: (1172, 1740),
                  4: (1740, 9999),
                  5: (9999, 99999)
                  }

        if any(row[88] == x for x in [None, 'None', '']):
            return 'L0000'

        if row[88].strip() == "NAV3GHZ":
            rf = 3
        elif row[88].strip() == "NAV9GHZ":
            rf = 9
        else:
            try:
                return self.map[row[88].rstrip()]
            except KeyError:
                return 'L0000'

        pris = [float(x) / 1000 for x in row[72:88] if float(x) > 0]
        stable = False if len(pris) > 1 else True

        pri = 0
        for key, rang in ranges.items():
            if all([x > rang[0] for x in pris]) and all([x < rang[1] for x in pris]):
                pri = key
                if pri == 5 and not stable:
                    pri = 0

        if pri == 0:
            return 'L0000'
        else:
            if not stable:
                pri += 5

            return '0' + str(pri) + str(rf) + 'NA'

    @staticmethod
    def round_time(dt):
        hr = int(dt.split(':')[0].split(' ')[-1])
        min_ = int(dt.split(':')[1])
        if min_ > 30:
            hr += 1
        return hr


class EOB(object):
    """
    EOB is a handler class for imported EOB data (Location data and elnot map data).

    Input:
    -----------
    EOB(file):         where file is the path to the EWDS Lite database for import. Can be a (.zip) containing database.

    Methods:
    -----------
    prep_file:          Unzips archive if needed
                            tempfile    = Temporary directory used for unziping archived data to
                            file        = name of EOB database file

    check_eob_date:     Checks the date of the data release. If out of the 35 day scope, will give a Yes/No popup to
                        import out-of-date data. User selectable.

    import_data:        Imports EOB data from the referenced database. Formats location and elnot map data for uploading
                        to local or networked PostgreSQL database.
                            data    = EOB location data for uploading
                            emitter = emitter-elnot mapping data used for mission data formatting.

    mainloop:           The main class loop for handling imported data. User can upload data, selet new files, display
                        help information, and exit the program from this main loop.

    convert_lat_long:   Static. Converts Lat/Long data from Degrees Minutes Seconds to Decimal Degrees.
    """

    def __init__(self, file, statusbar, parent):
        self.parent = parent
        self.statusbar = statusbar
        self.file = list(file)[0].split('/')[-1]
        self.path = '/'.join(list(file)[0].split('/')[:-1])
        self.valid = True
        self.tempfile = ''
        self.conn = None
        self.location_count = 0
        self.elnot_count = 0
        self.prep_file()
        self.check_eob_date()
        self.create_count()
        # PostgresqlDatabase('aea_eob_data_table').upload(self.data)
        # PostgresqlDatabase('emitter_elnot').upload(self.emitter)

    def prep_file(self):
        if self.file.endswith('.zip'):
            self.tempfile = TempDir().make()
            with zipfile.ZipFile(os.path.join(self.path, self.file), "r") as zipx:
                zipx.extractall(self.tempfile)

            for x in os.listdir(self.tempfile):
                self.file = x

    def check_eob_date(self):
        self.conn = win32com.client.Dispatch(r'ADODB.Connection')
        self.conn.Open('PROVIDER = Microsoft.ACE.OLEDB.12.0;DATA SOURCE = ' + os.path.join(self.tempfile, self.file)
                       if self.tempfile != '' else os.path.join(self.path, self.file) + ';')
        rs = win32com.client.Dispatch(r'ADODB.Recordset')
        rs.Open("SELECT database_date FROM db_information", self.conn, 1, 3)

        if not [next(iter(x)) for x in rs.GetRows(1, 0)][0].replace(tzinfo=None) > datetime.datetime.now() - \
                datetime.timedelta(days=32):
            ret = Handler().yes_no("Out-Of-Date Error",
                                   "This Database appears to be out-of-date. Would you still like to import?")
            if ret:
                rs.Close()
            else:
                rs.Close()
                self.valid = False

    def create_count(self):
        rs = win32com.client.Dispatch(r'ADODB.Recordset')
        rs.Open("SELECT * FROM emitter_location", self.conn, 1, 3)
        while not rs.EOF:
            self.location_count += 1
            rs.MoveNext()
        rs.Close()

        rs = win32com.client.Dispatch(r'ADODB.Recordset')
        rs.Open("SELECT * FROM emitter_elnot", self.conn, 1, 3)
        while not rs.EOF:
            self.elnot_count += 1
            rs.MoveNext()
        rs.Close()

    def read(self, table):
        rs = win32com.client.Dispatch(r'ADODB.Recordset')
        rs.Open("SELECT * FROM {}".format(table), self.conn, 1, 3)
        if table == 'emitter_location':
            while not rs.EOF:
                yield self.format_data([list(line).copy() for line in zip(*rs.GetRows(100, 0))])
            rs.Close()

        else:
            while not rs.EOF:
                yield [line[:2] for line in zip(*rs.GetRows(100, 0))]
            rs.Close()

    def format_data(self, rows):
        formatted_data = []
        for _L in rows:
            row = []
            row += _L[:3]
            row.append(self.convert_lat_long(_L[5:7]))
            row.append(self.convert_lat_long(_L[7:9]))
            row += _L[9:12]
            if isinstance(_L[15], str):
                row.append(_L[15].replace('"', "'"))
            else:
                row.append(_L[15])
            try:
                row.append(datetime.datetime.strptime(str(_L[25]).rstrip('+00:00'), '%Y-%m-%d %H:%M:%S').strftime(
                    '%Y-%m-%d %H:%M:%S'))
            except ValueError:
                row.append(str(
                    datetime.datetime.strptime(str(_L[25] + datetime.timedelta(seconds=1)).rstrip('+00:00'),
                                               '%Y-%m-%d %H:%M:%S')))
            formatted_data.append(row)
        return formatted_data

    def import_data(self):
        with StringIO() as outfile:
            with tqdm(total=self.location_count, file=outfile, desc='Updating Location Data ') as tq:
                rows = self.read('emitter_location')
                db = PostgresqlDatabase('aea_eob_data_table')
                while rows:
                    try:
                        db.upload(next(rows))
                        tq.update(100)
                        self.statusbar.set(outfile.getvalue().rstrip())
                        outfile.seek(0)
                        self.parent.update_idletasks()
                    except StopIteration:
                        break
        self.statusbar.set("")

        with StringIO() as outfile:
            with tqdm(total=self.elnot_count, file=outfile, desc='Updating Emitter Map Data ') as tq:
                rows = self.read('emitter_elnot')
                db = PostgresqlDatabase('emitter_elnot')
                while rows:
                    try:
                        db.upload(next(rows))
                        tq.update(100)
                        self.statusbar.set(outfile.getvalue().rstrip())
                        outfile.seek(0)
                        self.parent.update_idletasks()
                    except StopIteration:
                        break
        self.statusbar.set("")
        self.conn.Close()
        TempDir().remove(self.tempfile)
        self.statusbar.set("EOB Data Updated Successfully.")
        time.sleep(2)
        self.statusbar.set('')

    @staticmethod
    def convert_lat_long(dms):
        if len(dms[1]) < 7:
            dg = dms[1][:2]
            mn = dms[1][2:4]
            sc = dms[1][-2:]
        else:
            dg = dms[1][:3]
            mn = dms[1][3:5]
            sc = dms[1][-2:]
        dd = float(dg) + (float(mn) / 60.0) + (float(sc) / (60 * 60.0))
        if dms[0] == 'W' or dms[0] == 'S':
            dd *= -1

        return dd


class DatabaseMaintenance(object):
    def __init__(self, status):
        self.status = status
        self.check_pgpass()

    def check_pgpass(self):
        if os.path.isfile("C:/Users/{}/AppData/Roaming/postgresql/pgpass.conf".format(getpass.getuser())):
            with open("C:/Users/{}/AppData/Roaming/postgresql/pgpass.conf".format(getpass.getuser()), "w+") as file:
                file.write("{}:5432:*:postgres:P057GR35QLsuper!!".format(Config().get()['Default']['server']))
        else:
            os.mkdir("C:/Users/{}/AppData/Roaming/postgresql".format(getpass.getuser()))
            with open("C:/Users/{}/AppData/Roaming/postgresql/pgpass.conf".format(getpass.getuser()), "w+") as file:
                file.write("{}:5432:*:postgres:P057GR35QLsuper!!".format(Config().get()['Default']['server']))

    def dump(self, name_date, dir_=None):
        string = '"C:/Program Files/PostgreSQL/9.5/bin/pg_dump" -U postgres -h {} aea > "{}"' if os.path.isfile(
            r"C:/Program Files/PostgreSQL/9.5/bin/pg_dump") else \
            '"C:/EWDS-DIA/bin/pg_dump" -U postgres -h {} aea > "{}"'

        process = subprocess.Popen(string.format(Config().get()['Default']['server'], os.path.join(
            Config().get()['Default']['DatabaseDirectory'], name_date) if dir_ is None else
            os.path.join(dir_, name_date)), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, shell=True)

        spinner = itertools.cycle(['-', '/', '|', '\\'])
        out = process.poll()

        while out == process.poll():
            self.status.set("Generating Database Backup at {}...{}".format(
                Config().get()['Default']['DatabaseDirectory'] if dir_ is None else dir_,
                str(next(spinner))))

        self.status.set("Database Dump Complete.")
        time.sleep(1)
        self.status.set('')

    def backup(self):
        ret = Handler().yes_no("Database Dump", "Do have selected to backup your database.\nIf you want to select a new"
                                                " directory, select YES.\nTo use the default, select NO.")
        if not ret:
            if not any([x.endswith("Backup.sql") for x in os.listdir(Config().get()['Default']['DatabaseDirectory'])]):
                name_date = datetime.date.today().strftime("%Y-%m-%d")
                name_date = "GALE Database {} Backup.sql".format(name_date)
                self.dump(name_date)
            else:
                for x in os.listdir(Config().get()['Default']['DatabaseDirectory']):
                    if x.endswith("Backup.sql"):
                        self.dump(x)
                        os.rename(os.path.join(Config().get()['Default']['DatabaseDirectory'], x),
                                  os.path.join(Config().get()['Default']['DatabaseDirectory'],
                                               "GALE Database {} Backup.sql".format(
                                                   datetime.date.today().strftime("%Y-%m-%d"))))
        else:
            name_date = datetime.date.today().strftime("%Y-%m-%d")
            name_date = "GALE Database {} Backup.sql".format(name_date)
            self.dump(name_date, FileDialogs().directory())

    def restore(self):
        ret = Handler().yes_no("Database Restore", "WARNING:\n\nYou are about to restore your database, which will "
                               "overwrite any possible existing data.\nAre you sure you want to proceed?")
        if not ret:
            return

        try:
            conn = psycopg2.connect("dbname='postgres' user='gale' host='{}' password='GALEdatabase01!!'".format(
                Config().get()['Default']['server']))
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("CREATE DATABASE aea;")
            cur.close()
            conn.close()
        except psycopg2.ProgrammingError:
            conn = psycopg2.connect("dbname='postgres' user='gale' host='{}' password='GALEdatabase01!!'".format(
                Config().get()['Default']['server']))
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("DROP DATABASE aea;")
            cur.execute("CREATE DATABASE aea;")
            cur.close()
            conn.close()

        dir_ = FileDialogs().open({'filetypes': [('Database Backup Files', '.sql')]})[0]

        string = '"C:/Program Files/PostgreSQL/9.5/bin/psql" -h {} aea postgres < "{}"' if os.path.isfile(
            r"C:/Program Files/PostgreSQL/9.5/bin/psql") else \
            '"C:/EWDS-DIA/bin/psql" -h {} aea postgres < "{}"'

        process = subprocess.Popen(string.format(Config().get()['Default']['server'], dir_), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        spinner = itertools.cycle(['-', '/', '|', '\\'])
        out = process.poll()

        while out == process.poll():
            self.status.set("Restoring AEA database from {}...{}".format(dir_, str(next(spinner))))

        self.status.set("Database Restore Complete.")
        time.sleep(1)
        self.status.set('')


class StatusBar(Frame):
    def __init__(self, master):
        Frame.__init__(self, master)
        self.stringvar = StringVar()
        self.stringvar.set('')
        self.label = Label(self, textvariable=self.stringvar, relief='raised', anchor='nw')
        self.label.pack(fill='x')

    def set(self, arg):
        self.stringvar.set(arg)
        self.label.update_idletasks()

    def clear(self):
        self.stringvar.set('')
        self.label.update_idletasks()


class ConfigGui(object):
    def __init__(self):
        self.saved = False
        self.mainframe = Tk()
        self.files_dir = Entry(self.mainframe)
        self.db_dir = Entry(self.mainframe)
        self.server = Entry(self.mainframe)
        self.squadron = Entry(self.mainframe)
        self.callsign = Entry(self.mainframe)
        self.aor = Entry(self.mainframe)
        self.configs = None
        self.construct()
        self.mainframe.mainloop()

    def construct(self):
        self.mainframe.title('ADIT v{} Config Update'.format(Config().get()['Default']['version'] if
                                                             os.path.isdir('C:\EWDS-DIA') else 'UNKNOWN'))
        # self.main.geometry("720x250")
        # self.main.resizable(False, False)

        l_files_dir = Label(self.mainframe, text="Default Files Directory: ")
        l_db_dir = Label(self.mainframe, text="Default DB Backup Directory: ")
        l_server = Label(self.mainframe, text="Server Domain Name / IP: ")
        l_squadron = Label(self.mainframe, text="Squadron Number: ")
        l_callsign = Label(self.mainframe, text="Default Callsign: ")
        l_aor = Label(self.mainframe, text="Default AOR: ")

        labels = [l_files_dir, l_db_dir, l_server, l_squadron, l_callsign, l_aor]
        for index, label in enumerate(labels):
            label.grid(row=index, column=0, columnspan=2, sticky='E')

        self.files_dir.insert(0, Config().get()['Default']['filesdirectory'])
        self.db_dir.insert(0, Config().get()['Default']['databasedirectory'])
        self.server.insert(0, Config().get()['Default']['server'])
        self.squadron.insert(0, Config().get()['Default']['squadron'])
        self.callsign.insert(0, Config().get()['Default']['callsign'])
        self.aor.insert(0, Config().get()['Default']['aor'])

        self.configs = [self.files_dir, self.db_dir, self.server, self.squadron, self.callsign, self.aor]
        for index, entry in enumerate(self.configs):
            entry.grid(row=index, column=2, columnspan=3, sticky='W')

        fd_browse = Button(self.mainframe, text="Browse", command=self.fd_browse_set)
        # ToolTip(fd_browse, "Browse to Select Default File Directory")

        db_browse = Button(self.mainframe, text="Browse", command=self.db_browse_set)
        # ToolTip(db_browse, "Browse to Select Default Backup Directory")

        test = Button(self.mainframe, text="Test", command=self.test)
        # ToolTip(test, "Test Database IP/Domain Connection")

        save = Button(self.mainframe, text='Save Config', command=self.save_config)
        # ToolTip(save, "Saves Config Data.")

        reset = Button(self.mainframe, text='Reset to Defaults', command=self.reset_config)
        # ToolTip(reset, "Resets All Entries to Current Defaults.")

        save.grid(column=0, row=6)
        reset.grid(column=1, row=6)
        fd_browse.grid(column=5, row=0)
        db_browse.grid(column=5, row=1)
        test.grid(column=5, row=2)

        self.mainframe.protocol('WM_DELETE_WINDOW', self.exit)
        self.mainframe.focus_set()

    def fd_browse_set(self):
        dir_ = FileDialogs().directory(update=False)
        if dir_ != '':
            self.files_dir.delete(0, 'end')
            self.files_dir.insert(0, dir_)
        self.mainframe.focus_force()

    def db_browse_set(self):
        dir_ = FileDialogs().directory(update=False)
        if dir_ != '':
            self.db_dir.delete(0, 'end')
            self.db_dir.insert(0, dir_)
        self.mainframe.focus_force()

    def test(self):
        try:
            host = socket.gethostbyaddr(self.server.get())
            s = socket.create_connection((host[0], 5432), 2)
            s.close()
            Handler().info("Connection Test", "Connected to {} sucessfully.".format(self.server.get()))
            self.mainframe.focus_force()
        except (socket.gaierror, socket.herror, ConnectionRefusedError):
            Handler().show_error("Connection Test", "Unable to connect to {}.".format(self.server.get()))
            self.mainframe.focus_force()

    def save_config(self):
        if self.server.get() != Config().get()['Default']['server']:
            Config().save('Default', 'server', self.server.get())
            Config().update_server()

        Config().save('Default', 'filesdirectory', self.files_dir.get())
        Config().save('Default', 'databasedirectory', self.db_dir.get())
        Config().save('Default', 'squadron', self.squadron.get())
        Config().save('Default', 'callsign', self.callsign.get())
        Config().save('Default', 'aor', self.aor.get())
        Handler().info("Config Updated", "User Configuration Updated Successfully.")

    def reset_config(self):
        self.files_dir.delete(0, 'end')
        self.db_dir.delete(0, 'end')
        self.server.delete(0, 'end')
        self.squadron.delete(0, 'end')
        self.callsign.delete(0, 'end')
        self.aor.delete(0, 'end')

        self.files_dir.insert(0, Config().get()['Default']['filesdirectory'])
        self.db_dir.insert(0, Config().get()['Default']['databasedirectory'])
        self.server.insert(0, Config().get()['Default']['server'])
        self.squadron.insert(0, Config().get()['Default']['squadron'])
        self.callsign.insert(0, Config().get()['Default']['callsign'])
        self.aor.insert(0, Config().get()['Default']['aor'])

    def verify_data(self):
        defaults = ['filesdirectory', 'databasedirectory', 'server', 'squadron', 'callsign', 'aor']
        return all(c.get() == d for c, d in
                   zip(self.configs, [Config().get()['Default'][default] for default in defaults]))

    def exit(self):
        if not self.verify_data():
            ret = Handler().yes_no("Unsaved Data", "You have unsaved configuration data.\n"
                                                   "Are you sure you want to exit?")
            if ret:
                self.mainframe.destroy()
            else:
                self.mainframe.focus_force()
        else:
            self.mainframe.destroy()


class GUI(object):
    def __init__(self):
        super(GUI, self).__init__()
        self.buffer = 1024 * 1024
        self.main = Tk()
        self.eob = None
        self.item_vars = None
        self.item_labels = None
        self.statusbar = StatusBar(self.main)
        self.listbox = None
        self.missions = {}
        self.contruct()
        self.main.protocol("WM_DELETE_WINDOW", self._exit)
        self.main.mainloop()

    def contruct(self):
        self.main.title('ADIT v{}'.format(Config().get()['Default']['version']
                                          if os.path.isdir('C:\EWDS-DIA') else 'UNKNOWN'))
        self.main.geometry("720x250")
        self.main.resizable(False, False)

        menubar = Menu(self.main)

        file_menu = Menu(menubar, tearoff=0)
        file_menu.add_command(label='Open', command=lambda: self.thread_func(self.select_mission_data))
        file_menu.add_separator()
        file_menu.add_command(label='Exit', command=self._exit)
        menubar.add_cascade(label='File', menu=file_menu)

        edit_menu = Menu(menubar, tearoff=0)
        edit_menu.add_command(label='Update Config', command=lambda: self.thread_func(self.update_config))
        menubar.add_cascade(label='Edit', menu=edit_menu)

        eob_menu = Menu(menubar, tearoff=0)
        eob_menu.add_command(label='Upload EOB Data', command=lambda: self.thread_func(self.upload_eob_data))
        menubar.add_cascade(label='EOB Data', menu=eob_menu)

        db_menu = Menu(menubar, tearoff=0)
        db_menu.add_command(label='Backup Database', command=lambda: self.thread_func(self.backup_database))
        db_menu.add_command(label='Restore Database', command=lambda: self.thread_func(self.restore_database))
        menubar.add_cascade(label="DB Maintenance", menu=db_menu)

        mainframe = Frame(self.main)
        mainframe.pack(side='top', anchor='w', fill='x')

        small_font = font.Font(self.main, size=8)
        large_font = font.Font(self.main, size=12)

        self.listbox = Listbox(mainframe, selectmode=MULTIPLE, height=8, font=large_font)
        select_all = Button(mainframe, text='Select All', command=self.select_all)
        ToolTip(select_all, "Selects All Missions Within the List Window")

        deselect_all = Button(mainframe, text='Deselect All', command=self.deselect_all)
        ToolTip(deselect_all, "Deselects All Missions Within the List Window")

        export = Button(mainframe, text='Export', command=lambda: self.thread_func(self.write_data))
        ToolTip(export, "Write All Selected Files to '.csv' Files")

        upload = Button(mainframe, text='Upload', command=lambda: self.thread_func(self.upload_data))
        ToolTip(upload, "Upload All Selected Files to Associated GALE PostgreSQL Database")

        frame = Frame(mainframe)

        self.listbox.grid(column=0, row=0, columnspan=4, rowspan=8, ipadx=120, sticky='NW')
        frame.grid(column=4, row=0, columnspan=2, rowspan=8, sticky='NW')
        select_all.grid(column=0, row=8)
        deselect_all.grid(column=1, row=8)
        export.grid(column=2, row=8)
        upload.grid(column=3, row=8)

        self.item_vars = {
            'label0': StringVar(),
            'label1': StringVar(),
            'label2': StringVar(),
            'label3': StringVar(),
            'label4': StringVar(),
            'label5': StringVar(),
            'label6': StringVar(),
            'label7': StringVar()
        }

        self.item_labels = {
            'label0': Label(frame, textvariable=self.item_vars['label0'], font=small_font),
            'label1': Label(frame, textvariable=self.item_vars['label1'], font=small_font),
            'label2': Label(frame, textvariable=self.item_vars['label2'], font=small_font),
            'label3': Label(frame, textvariable=self.item_vars['label3'], font=small_font),
            'label4': Label(frame, textvariable=self.item_vars['label4'], font=small_font),
            'label5': Label(frame, textvariable=self.item_vars['label5'], font=small_font),
            'label6': Label(frame, textvariable=self.item_vars['label6'], font=small_font),
            'label7': Label(frame, textvariable=self.item_vars['label7'], font=small_font)
        }

        for x in range(8):
            self.item_labels['label{}'.format(x)].grid(column=0, row=x, columnspan=2, sticky='nw')

        self.statusbar.pack(side='bottom', fill='x')
        self.main.focus_set()
        self.main.config(menu=menubar)

    def upload_eob_data(self):
        eob_dir = FileDialogs().open({'filetypes': [('EOB Files', '.mdb .accdb .zip')]})
        if eob_dir is not None:
            self.eob = EOB(eob_dir, self.statusbar, self.main)
            if self.eob.valid:
                self.eob.import_data()

    def select_all(self):
        self.listbox.select_set(0, 'end')

    def deselect_all(self):
        self.listbox.select_clear(0, 'end')

    def update_config(self):
        ConfigGui()

    def backup_database(self):
        db = DatabaseMaintenance(self.statusbar)
        db.backup()

    def restore_database(self):
        db = DatabaseMaintenance(self.statusbar)
        db.restore()

    def select_mission_data(self):
        files = FileDialogs().open()
        if files is None:
            return

        for index, value in enumerate(self.listbox.get(0, 'end')):
            TempDir().remove(self.missions[index].tempfile)

        self.listbox.delete(0, 'end')

        for index, value in self.item_vars.items():
            value.set('')

        self.missions = {}

        for index, file in enumerate(files):
            if 'EWDS' in file and 'Lite' in file:
                Handler().show_error("Data Type Error", "'{}' is an EOB File, not Mission Data File.".format(file))
                continue
            self.statusbar.set("Generating Data Characteristics for {}...".format(file.split('/')[-1]))
            mission = Data(file)
            if mission.valid:
                self.missions[index] = mission
                self.listbox.insert(index, mission.filename)
        self.statusbar.clear()

    def generate_file_name(self, mission):
        return "{}_{}_{}_{}{}{}_{}_S".format(mission.date, Config().get()['Default']['squadron'],
                                             Config().get()['Default']['callsign'], mission.start_time,
                                             mission.stop_time, mission.buno, Config().get()['Default']['aor'])

    def update_defaults(self, path):
        name = path.split('/')[-1]
        values = name.split('_')

        squadron = values[1]
        callsign = values[2]
        aor = values[4]

        Config().save('Default', 'squadron', squadron)
        Config().save('Default', 'callsign', callsign)
        Config().save('Default', 'aor', aor)

    def write_data(self):
        for index, value in [(idx, self.listbox.get(idx)) for idx in self.listbox.curselection()]:
            self.statusbar.set("Saving {} as Zipped CSV...".format(value))
            mission = self.missions[index]
            save_dir = FileDialogs().save(self.generate_file_name(mission))
            if save_dir == '':
                continue

            save_name = save_dir.split('/')[-1].replace('.zip', '')
            tempdir = TempDir().make()

            rows = mission.read(self.buffer)
            with open(os.path.join(tempdir, "{}.csv".format(save_name)), 'w') as f:
                with StringIO() as outfile:
                    with tqdm(total=mission.size, unit='bytes', unit_scale=True, file=outfile) as tq:
                        while rows:
                            try:
                                csv.writer(f, lineterminator='\n').writerows(next(rows))
                                if mission.mimetype == 'csv':
                                    tq.update(self.buffer)
                                else:
                                    tq.update(20)
                                self.item_vars['label{}'.format(index)].set(outfile.getvalue())
                                outfile.seek(0)
                                self.main.update_idletasks()
                            except StopIteration:
                                self.item_vars['label{}'.format(index)].set('Compressing {}...'.format(
                                    mission.filename))
                                break

            with zipfile.ZipFile(save_dir, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(os.path.join(tempdir, "{}.csv".format(save_name)),
                           os.path.basename(os.path.join(tempdir, "{}.csv".format(save_name))))

            TempDir().remove(tempdir)
            self.update_defaults(save_dir)
            PDFgenerator(mission, save_name, save_dir)
            self.item_vars['label{}'.format(index)].set('Export Finished.')
        self.statusbar.clear()

    def upload_data(self):
        for index, value in [(idx, self.listbox.get(idx)) for idx in self.listbox.curselection()]:
            self.statusbar.set("Writing {} data to {}".format(value, Config().get()['Default']['Server']))
            mission = self.missions[index]
            rows = mission.read(1024 * 1024)
            with StringIO() as outfile:
                with tqdm(total=mission.size, unit='bytes', unit_scale=True, file=outfile) as tq:
                    db = PostgresqlDatabase('aea_aef_data_table')
                    while rows:
                        try:
                            db.upload(next(rows))
                            if mission.mimetype == 'csv':
                                tq.update(1024 * 1024)
                            else:
                                tq.update(100)

                            self.item_vars['label{}'.format(index)].set(outfile.getvalue())
                            outfile.seek(0)
                            self.main.update_idletasks()
                        except StopIteration:
                            self.item_vars['label{}'.format(index)].set('Upload Finished.')
                            self.statusbar.set('Deleting database duplicate entries...')
                            break

                    self.statusbar.clear()
                    db.delete_duplicates()

    def _exit(self):
        try:
            for _, value in self.missions.items():
                try:
                    TempDir().remove(value.tempfile)
                except FileNotFoundError:
                    pass
        except PermissionError:
            Handler().show_error("Process Running", "Cannot Exit System While Other Process is Running.")
            return

        try:
            if self.eob:
                try:
                    TempDir().remove(self.eob.tempfile)
                except FileNotFoundError:
                    pass
        except PermissionError:
            Handler().show_error("Process Running", "Cannot Exit System While Other Process is Running.")
            return

        self.main.destroy()
        sys.exit(0)

    @staticmethod
    def thread_func(func, *args):
        thread = threading.Thread(target=func, args=args)
        thread.daemon = False
        thread.start()


if __name__ == '__main__':
    GUI()
