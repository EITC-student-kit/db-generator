__author__ = 'Kristo Koert'

from sqlite3 import connect, OperationalError, PARSE_DECLTYPES
import os
from datetime import datetime

def bench_mark(func):
    """Prints how much time a function took."""
    def new_f(*args):
        start = datetime.now()
        func(*args)
        end = datetime.now()
        print("Running ", func.__name__, " took:", end - start)
    return new_f


def ical_datetime_to_timestamp(ical_dt):
    """
    :param ical_dt: i.e. "20140508T143000Z"
    :rtype Timestamp
    """
    from sqlite3 import Timestamp
    ical_dt = ical_dt[ical_dt.find(':') + 1:].replace("T", "")
    #Todo fix in main
    return Timestamp(int(ical_dt[:4]), int(ical_dt[4:6]), int(ical_dt[6:8]), int(ical_dt[8:10]) + 2, int(ical_dt[10:12]))


class DataTypesAbstractClass():
    """Any classes inheriting from this class would be meant for creating instances that can be easily written to
    database, created from database rows or add the ability to safely and easily remove instances from database"""

    def __init__(self):
        self._db_row = []

    def _create_database_row(self, *kwargs):
        if len(self._db_row) == 0:
            self._db_row = kwargs

    def get_database_row(self):
        return self._db_row


class Activity(DataTypesAbstractClass):

    def __init__(self, type_of, start, end, time_spent):
        """
        :param type_of: Either Productive, Neutral of Counterproductive
        :type type_of: str
        :type start: datetime
        :type end: datetime
        :type time_spent: int
        """
        DataTypesAbstractClass.__init__(self)
        self._create_database_row(type_of, start, end, time_spent)


class AClass(DataTypesAbstractClass):

    def __init__(self, subject_code, subject_name, attending_groups, class_type, start_timestamp, end_timestamp,
                 classroom, academician, attendible=False):
        """
        :param subject_code: The subjects code (e.g. I241).
        :type subject_code: str
        :param subject_name: The name of the class.
        :type subject_name: str
        :param attending_groups: Attending groups separated by comas.
        :type attending_groups: str
        :param class_type: Lecture, Exercise, Practice, Repeat prelim, Reservation, Consultation etc.
        :type class_type: str
        :param start_timestamp: Class starts at.
        :type start_timestamp: Timestamp
        :param end_timestamp: Class ends at.
        :type end_timestamp: Timestamp
        :param classroom: Where class takes place.
        :type classroom: str
        :param academician: The academician(s), format separated with comas.
        :type academician: str
        :param attendible: Does the user attend this class or not
        :type attendible: bool
        """
        DataTypesAbstractClass.__init__(self)
        self._create_database_row(subject_code, subject_name, attending_groups, class_type, start_timestamp,
                                  end_timestamp, classroom, academician, attendible)

    def __str__(self):
        return str(self.get_database_row())

DATABASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/itckitdb"

dt = datetime.now()
activ_cls = Activity('', dt, dt, 1).__class__
a_cls_cls = AClass('', '', '', '', dt, dt, '', '', False).__class__

table_dict = {activ_cls: ("Activity", "(?,?,?,?)"),
              a_cls_cls: ("Class", "(?,?,?,?,?,?,?,?,?)")}

def add_to_db(data_type):
    """Adds instances from datatype to correct table_name.
    :type data_type Iterable | AClass | Activity | Notification
    """
    try:
        iter(data_type)
    except TypeError:
        data_type = [data_type]
    db = connect_to_db()
    cls = data_type[0].__class__
    table_name = table_dict[cls][0]
    db_coloumns = table_dict[cls][1]
    db.executemany(
        "INSERT INTO " + table_name + " VALUES "
        + db_coloumns, [cls.get_database_row() for cls in data_type])
    db.commit()

def connect_to_db():
    """rtype: Connection"""
    db = connect(DATABASE_PATH, detect_types=PARSE_DECLTYPES)
    attempt_tables_creation(db.cursor())
    return db

def get_all_classes():
    """:rtype tuple"""
    db = connect_to_db()
    return db.cursor().execute("SELECT * FROM Class").fetchall()

def get_attendible_classes():
    db = connect_to_db()
    return db.cursor().execute("SELECT * FROM Class WHERE user_attends = 1").fetchall()

def get_all_activities():
    """:rtype Iterable"""
    conn = connect_to_db()
    return conn.cursor().execute("SELECT * FROM Activity").fetchall()


def remove_all_activities():
    db = connect_to_db()
    db.execute("DELETE * FROM Activity")
    db.commit()

def attempt_tables_creation(cursor):
    """If tables do not yet exist, they are created."""
    #ToDo implement a check
    try:
        cursor.execute("""CREATE TABLE Class (subject_code TEXT, subject_name TEXT, attending_groups TEXT,
                                class_type TEXT, start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, classroom TEXT,
                                academician TEXT, user_attends BOOLEAN)""")
    except OperationalError:
        #Already exists
        pass
    try:
        cursor.execute("""CREATE TABLE Activity (activity_type TEXT, start_timestamp TIMESTAMP,
                                end_timestamp TIMESTAMP, spent_time INTEGER )""")
    except OperationalError:
        #Already exists
        pass


class ICalParser():

    _keywords = ["Subject code: ", "Groups: ", "Type: ", "DTSTART:", "DTEND:", "SUMMARY:",
                 "LOCATION:", "Academician: ", "Attendible: "]

    _parameters = {key: [] for key in _keywords}
    classes = []

    def __init__(self):
        import os
        self.user_ical_file = open(os.path.dirname(os.path.abspath(__file__)) + "/user_ical", "r")
        self.main_ical_file = open(os.path.dirname(os.path.abspath(__file__)) + "/main_ical", "r")

    def _set_parameters(self):
        """Collects all the AClass object creation parameters and stores them."""
        vevents = self._extract_vevents()

        for event in vevents:
            event = event.split('\n')
            [event.append(element) for element in event[8].replace("DESCRIPTION:", '').split('\\n')]
            del event[8]
            self._find(event)

    def _extract_vevents(self):
        """Returns a list of ical vevent lines.
        :rtype :list"""
        found_start = found_end = False
        vevent = ""
        vevents = []
        #ToDo implement for both main_ical and user_ical
        for ical_file in [self.main_ical_file, self.user_ical_file]:
            for line in ical_file:
                if "BEGIN:VEVENT" in line:
                    found_start = True
                if "END:VEVENT" in line:
                    found_end = True
                if found_start:
                    vevent += line
                if found_start and found_end:
                    vevents.append(vevent)
                    if ical_file == self.main_ical_file:
                        vevents[-1] += "\nfrom main"
                    else:
                        vevents[-1] += "\nfrom user"
                    found_start = False
                    found_end = False
                    vevent = ""
        return vevents

    def _find(self, event):
        """Finds and sets all the parameters in a line, if there are any.
        :param event A vevent string from a ical file
        :type event: str
        """
        var = None
        for line in event:
            for key in self._keywords:
                if key in line:
                    var = line[line.index(key):].replace(key, '')
                    if key == "DTSTART:" or key == "DTEND:":
                        self._parameters[key].append(ical_datetime_to_timestamp(var))
                    else:
                        self._parameters[key].append(var.replace('\\', ''))
                #ToDo replace temporary fix
                elif key == "Academician: " and len(self._parameters["DTSTART:"]) > len(self._parameters["Academician: "]):
                    self._parameters[key].append('')
            if "from main" in line:
                self._parameters["Attendible: "].append(False)
            elif "from user" in line:
                self._parameters["Attendible: "].append(True)

    def _create_class_instances(self):
        par = self._parameters
        for i in range(len(self._parameters["DTSTART:"])):
            #SUMMARY: returns name
            self.classes.append(AClass(par["SUMMARY:"][i], par["Subject code: "][i], par["Groups: "][i],
                                       par["Type: "][i], par["DTSTART:"][i], par["DTEND:"][i], par["LOCATION:"][i],
                                       par["Academician: "][i], par["Attendible: "][i]))

    def get_classes(self):
        """Gets all the parameters and creates instances.
        :rtype list
        """
        self._set_parameters()
        self._create_class_instances()
        return self.classes

#----------------------------------------------------------
#Working Code

from random import randint
dt = datetime.now()
types = ["Productive", "Neutral", "Counterproductive"]


def fill_activity_table():
    """Fills activity table with 100 random Activity instances."""
    generator = [Activity(types[randint(0, 2)], dt, dt, randint(1, 500)) for i in range(1000)]
    add_to_db(generator)

icp = ICalParser()
classes = icp.get_classes()

@bench_mark
def fill_class_table():
    """Fills Class table with classes. From user ical are attendible rest not attendible."""
    global classes
    add_to_db(classes)

if __name__ == "__main__":
    fill_activity_table()
    fill_class_table()
    #If you want to print the tables content
    #[print(act) for act in get_all_activities()]
    [print(cls) for cls in get_all_classes()]
    #print(results[0], results[-1])