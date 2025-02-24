import json
import datetime


class death:
    def __init__(self, line: str):
        full_name = line[0:80].split("*")

        self.family_name = full_name[0]
        self.surname = full_name[1].split("/")[0]

        self.sexe = int(line[80])

        self.birthdate = datetime.date(int(line[81:85]), int(line[85:87]), int(line[87:89]))

        self.birthplace_code = line[89:94]
        self.birthplace_name = line[94:124].strip()
        self.birth_country = line[124:154].strip()
        
        self.death_date = datetime.date(int(line[154:158]), int(line[158:160]), int(line[160:162]))
        self.death_place = line[162:168]
        self.num_acte_deces = line[168:177]



if __name__ == "__main__":
    line = "DUCRET*MARIE ANTOINETTE/                                                        21922010901004AMBERIEU-EN-BUGEY                                           19701210014216                              "
    print(death(line).__dict__)