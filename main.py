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


#New util function

def ical_datetime_to_timestamp(ical_dt):
    """
    :param ical_dt: i.e. "20140508T143000Z"
    :rtype Timestamp
    """
    from sqlite3 import Timestamp
    ical_dt = ical_dt[ical_dt.find(':') + 1:].replace("T", "")
    return Timestamp(int(ical_dt[:4]), int(ical_dt[4:6]), int(ical_dt[6:8]), int(ical_dt[8:10]) + 2, int(ical_dt[10:12]))


class DataTypesAbstractClass():
    """Any classes inheriting from this class would be meant for creating instances that can be easily written to
    database, created from database rows or add the ability to safely and easily remove instances from database"""

    def __init__(self):
        self._db_row = []

    def _create_database_row(self, *kwargs):
        """Sets the supplied parameters as the value for instances database row representation. Only works if a
        database row has not already been created, thus ensuring that inheritance can be used."""
        if len(self._db_row) == 0:
            self._db_row = kwargs

    def get_database_row(self):
        return self._db_row

    def __eq__(self, other):
        return self.get_database_row() == other.get_database_row()

    def __str__(self):
        return str(self.get_database_row())


class Activity(DataTypesAbstractClass):

    def __init__(self, type_of, start, end, spent_time):
        """The database table -> Activity (activity_type TEXT, start_timestamp TIMESTAMP, end_timestamp TIMESTAMP,
         spent_time INTEGER )

        :param type_of: Either Productive, Neutral of Counterproductive
        :type type_of: str
        :type start: datetime
        :type end: datetime
        :type spent_time: int
        """
        try:
            assert type_of in ("Productive", "Neutral", "Counterproductive")
        except AssertionError:
            print("Invalid parameter passed for type_of in Activity instance creation: ", type_of)

        DataTypesAbstractClass.__init__(self)
        self._create_database_row(type_of, start, end, spent_time)


class AClass(DataTypesAbstractClass):

    def __init__(self, subject_code, subject_name, attending_groups, class_type, start_timestamp, end_timestamp,
                 classroom, academician, attendible=False):
        """The database table -> Class (subject_code TEXT, subject_name TEXT, attending_groups TEXT,
                                class_type TEXT, start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, classroom TEXT,
                                academician TEXT, user_attend BOOLEAN)

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

    def __eq__(self, other):
        """Last element attendible row in database can differ and still be the same description."""
        return self.get_database_row()[:-1] == other.get_database_row()[:-1]


DATABASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/itckitdb"

dt = datetime.now()
activ_cls = Activity('Productive', dt, dt, 1).__class__
a_cls_cls = AClass('', '', '', '', dt, dt, '', '', False).__class__

table_dict = {activ_cls: ("Activity", "(?,?,?,?)"),
              a_cls_cls: ("Class", "(?,?,?,?,?,?,?,?,?)")}


def add_to_db(datatypes):
    """Adds instances from datatype to correct table. Duplicates are not written.
    :type datatypes Iterable | DataTypesAbstractClass
    """
    new = []
    try:
        iter(datatypes)
    except TypeError:
        datatypes = [datatypes]
    if len(datatypes) == 0:
        return
    db = connect_to_db()
    cls = datatypes[0].__class__
    table_name = table_dict[cls][0]
    db_coloumns = table_dict[cls][1]
    new = get_not_already_in_db(datatypes, table_name)
    db.executemany(
        "INSERT INTO " + table_name + " VALUES "
        + db_coloumns, [cls.get_database_row() for cls in new])
    db.commit()


def get_not_already_in_db(datatypes, table_name):
    new = []
    if table_name == "Class":
        currently_in_db = get_all_classes()
    else:
        return datatypes
    for datatype in datatypes:
        if datatype not in currently_in_db:
            new.append(datatype)
    return new


def connect_to_db():
    """rtype: Connection"""
    db = connect(DATABASE_PATH, detect_types=PARSE_DECLTYPES)
    attempt_tables_creation(db.cursor())
    return db


def get_all_classes():
    """Not used outside testing. Returns database rows not instances of objects.
    :rtype tuple"""
    db = connect_to_db()
    db_rows = db.cursor().execute("SELECT * FROM Class").fetchall()
    clss = []
    for r in db_rows:
        clss.append(AClass(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]))
    return clss


def get_all_activities():
    """Not used outside testing. Returns database rows not instances of objects.
    :rtype Iterable"""
    conn = connect_to_db()
    return conn.cursor().execute("SELECT * FROM Activity").fetchall()


def remove_all_activities():
    db = connect_to_db()
    db.execute("DELETE * FROM Activity")
    db.commit()


def attempt_tables_creation(cursor):
    """If tables do not yet exist, they are created."""
    #ToDo implement a real check?
    try:
        cursor.execute("""CREATE TABLE Class (subject_code TEXT, subject_name TEXT, attending_groups TEXT,
                                class_type TEXT, start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, classroom TEXT,
                                academician TEXT, user_attend BOOLEAN)""")
    except OperationalError:
        #Already exists
        pass
    try:
        cursor.execute("""CREATE TABLE Activity (activity_type TEXT, start_timestamp TIMESTAMP,
                                end_timestamp TIMESTAMP, spent_time INTEGER )""")
    except OperationalError:
        #Already exists
        pass

keywords = ["Subject code: ", "Groups: ", "Type: ", "DTSTART:", "DTEND:", "SUMMARY:",
            "LOCATION:", "Academician: "]


def _all_parameters_equal(parameters):
    """Checks if all the parameters are of equal length.
    :type parameters: dict
    :raises AssertionError"""
    number_of_events = len(parameters["DTSTART:"])
    for key in keywords:
        try:
            assert number_of_events == len(parameters[key])
        except AssertionError:
            print("Parameters are not of equal length.")
            [print(params, "->", len(parameters[params])) for params in parameters]
            raise RuntimeError


def _format_parameters(old_parameters):
    """Parameters are converted to their proper forms.
    :type old_parameters: dict
    :rtype: dict"""
    new_parameters = {key: [] for key in keywords}
    for el in old_parameters["Groups: "]:
        new_parameters["Groups: "].append(el.replace('\\', ''))
    for el in old_parameters["SUMMARY:"]:
        new_parameters["SUMMARY:"].append(el[:el.find('[')])
    for el in old_parameters["DTEND:"]:
        new_parameters["DTEND:"].append(ical_datetime_to_timestamp(el))
    for el in old_parameters["DTSTART:"]:
        new_parameters["DTSTART:"].append(ical_datetime_to_timestamp(el))
    for key in keywords:
        if len(new_parameters[key]) == 0:
            new_parameters[key] = old_parameters[key]
    return new_parameters


def _collect_parameters(formatted_ical_text, parameters):
    """Recursively collects all the parameters
    :type formatted_ical_text: str
    :type parameters dict
    :rtype: dict
    """
    try:
        cut_off = formatted_ical_text.index("DTSTART:", 1)
        event = formatted_ical_text[0:cut_off]
        rest = formatted_ical_text[cut_off:]
        #Deals with events that do not have a Academician
        if len(event.split('\n')) == 8:
            event += "Academician: "
        for line in event.split('\n'):
            for key in parameters.keys():
                if key in line:
                    parameters[key].append(line.replace(key, ''))
        return _collect_parameters(rest, parameters)
    except ValueError:
        parameters = _format_parameters(parameters)
        _all_parameters_equal(parameters)
        return parameters


def _combine_classes(user_classes, main_classes):
    """Returns a list of only the AClass objects that are unique.
    :type user_classes: list
    :type main_classes: list
    :rtype: list
    """
    for cls in user_classes:
        if cls in main_classes:
            main_classes[main_classes.index(cls)] = cls
    return main_classes


def parse_icals():
    """Parses ical files and writes the results to database."""
    parameters_dict = {key: [] for key in keywords}
    user_classes = []
    main_classes = []

    user_ical = open(os.path.dirname(os.path.abspath(__file__)) + "/user_ical", "r").read()
    main_ical = open(os.path.dirname(os.path.abspath(__file__)) + "/main_ical", "r").read()

    parameters = _collect_parameters(user_ical, parameters_dict)
    for i in range(len(parameters["DTSTART:"])):
        user_classes.append(AClass(parameters["Subject code: "][i], parameters["SUMMARY:"][i], parameters["Groups: "][i],
                                   parameters["Type: "][i], parameters["DTSTART:"][i], parameters["DTEND:"][i],
                                   parameters["LOCATION:"][i], parameters["Academician: "][i], True))

    parameters.clear()

    parameters = _collect_parameters(main_ical, parameters_dict)
    for i in range(len(parameters["DTSTART:"])):
        main_classes.append(AClass(parameters["Subject code: "][i], parameters["SUMMARY:"][i], parameters["Groups: "][i],
                                   parameters["Type: "][i], parameters["DTSTART:"][i], parameters["DTEND:"][i],
                                   parameters["LOCATION:"][i], parameters["Academician: "][i], False))

    return _combine_classes(user_classes, main_classes)

#----------------------------------------------------------
#Working Code

from random import randint
dt = datetime.now()
types = ["Productive", "Neutral", "Counterproductive"]

@bench_mark
def fill_activity_table():
    """Fills activity table with 100 random Activity instances."""
    generator = [Activity(types[randint(0, 2)], dt, dt, randint(1, 500)) for i in range(1000)]
    add_to_db(generator)


@bench_mark
def fill_class_table():
    """Fills Class table with classes. From user ical are attendible rest not attendible."""
    add_to_db(parse_icals())

if __name__ == "__main__":
    fill_activity_table()
    fill_class_table()
    #If you want to print the tables content
    #[print(act) for act in get_all_activities()]
    #[print(cls) for cls in get_all_classes()]
    #print(len(get_all_classes()))
    #print(results[0], results[-1])