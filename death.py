import json
import datetime


class death:
    def __init__(self, line: (str)):
        # The input is a tuple that we split by hand
        # because the pattern is too complex to be handled by Spark
        line = line[0]
        full_name = line[0:80].split("*")

        self.family_name = full_name[0]
        # If the name does not fit in the 80 characters,
        # then there is no "*", and thus no surname
        try:
            self.surname = full_name[1].split("/")[0]
        except IndexError:
            self.surname = ""

        self.sexe = int(line[80])

        try: 
            self.birthdate = datetime.date(int(line[81:85]), int(line[85:87]), int(line[87:89]))
        except ValueError:
            # For performance reasons, we do that if the normal approach failed.
            # The 0 indicates that the date is unknown, wu set an arbitrary date.
            # Note that this will still fail if we don
            self.birthdate = datetime.date(int(line[81:85]), max(1,int(line[85:87])), max(1, int(line[87:89])))

        self.birthplace_code = line[89:94]
        self.birthplace_name = line[94:124].strip()
        self.birth_country = line[124:154].strip()
        
        try:
            self.death_date = datetime.date(int(line[154:158]), int(line[158:160]), int(line[160:162]))
        except ValueError:
            # Death is necessarily after birth, at least in the records
            self.death_date = datetime.date(int(line[154:158]), max(self.birthdate.month,int(line[158:160])), max(self.birthdate.day,int(line[160:162])))

        self.death_place = line[162:168]
        self.num_acte_deces = line[168:177]



if __name__ == "__main__":
    line = "DUCRET*MARIE ANTOINETTE/                                                        21922010901004AMBERIEU-EN-BUGEY                                           19701210014216                              "
    print(death([line]).__dict__)