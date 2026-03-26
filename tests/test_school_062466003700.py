from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import unittest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scrapers"
    / "schools"
    / "ca"
    / "062466003700.py"
)
SPEC = spec_from_file_location("school_062466003700", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load scraper module from {MODULE_PATH}")
livingston = module_from_spec(SPEC)
SPEC.loader.exec_module(livingston)


JVV_DOC_SAMPLE = """2025 JV/Varsity Football Schedule
LIVINGSTON HIGH SCHOOL
7/14 SJS Limited Period - 7/28 First Day of Practice
__________________________________________________________________________
Day/Date        Opponent                Location              Depart Time                 Game Time
Fri. 8/15        Modesto High                Modesto                                                TBD
Thurs. 8/21        Le Grand (JV)                Le Grand                                        7:00 pm
Fri. 8/22        Le Grand (Varsity)        Le Grand                                        8:00 pm
Fri. 8/29        *Sonora                Livingston                                        5:00/7:00 pm
Fri. 9/12        ---------------------        BYE       ---------------------


Contacts
Steven Wine                Head Coach                stwine@muhsd.org
Scott Winton                Athletic Director                swinton@muhsd.org
Charles Jolly                Principal                 cjolly@muhsd.org
Livingston High                 Livingston, CA.95334        http://lhs.muhsd.org/lhs
Athletic Department            1617 Main Street         (209) 325-2663 / (209) 325-2600
"""


FLAG_DOC_SAMPLE = """JV/Varsity Girls Flag Football 2026 Schedule
LIVINGSTON HIGH SCHOOL
Day/Date        Opponent            Level                Location            Depart Time        Game Time (JV / Var)
Fri 8/14-8/15
Jamboree Scrimmage
JV/Var
Rocklin
TBD
6:00 / 7:00 PM
Tues 8/18
(Foundation)
Varsity
TBD
TBD
TBD
Thurs 8/20
Sonora
Var
Livingston HS
NA
6:00 PM
All home game played at the LHS Stadium


Jacob Ayala                 Head Coach                jacobjayala29@outlook.com
Scott Winton                Athletic Director                swinton@muhsd.org
"""


ATHLETIC_DIRECTOR_TEXT = """Athletic Director
Scott Winton
Athletic Director
Phone:
209-325-2665
Our mission at LHS is to provide a rigorous athletic program."""


class LivingstonScraperParsingTests(unittest.TestCase):
    def test_parse_standard_doc_extracts_schedule_and_contacts(self):
        parsed = livingston._parse_doc(  # noqa: SLF001
            JVV_DOC_SAMPLE,
            team_label="JV/V Football",
            doc_url="https://docs.google.com/document/d/example/edit?usp=sharing",
        )

        self.assertEqual(parsed["season"], "2025")
        self.assertEqual(parsed["row_count"], 5)
        self.assertEqual(parsed["schedule_rows"][0]["opponent"], "Modesto High")
        self.assertEqual(parsed["schedule_rows"][0]["location"], "Modesto")
        self.assertEqual(parsed["schedule_rows"][3]["opponent"], "*Sonora")
        self.assertEqual(parsed["contacts"][0]["name"], "Steven Wine")
        self.assertEqual(parsed["contacts"][0]["role"], "Head Coach")
        self.assertEqual(parsed["contacts"][1]["email"], "swinton@muhsd.org")
        self.assertEqual(parsed["department_address"], "1617 Main Street")
        self.assertEqual(parsed["department_phone"], "(209) 325-2663 / (209) 325-2600")

    def test_parse_flag_doc_extracts_rows_and_home_venue_note(self):
        parsed = livingston._parse_doc(  # noqa: SLF001
            FLAG_DOC_SAMPLE,
            team_label="Girls Flag Football",
            doc_url="https://docs.google.com/document/d/example2/edit?usp=sharing",
        )

        self.assertEqual(parsed["season"], "2026")
        self.assertEqual(parsed["row_count"], 3)
        self.assertEqual(parsed["schedule_rows"][0]["opponent"], "Jamboree Scrimmage")
        self.assertEqual(parsed["schedule_rows"][0]["location"], "Rocklin")
        self.assertEqual(parsed["schedule_rows"][2]["game_time"], "6:00 PM")
        self.assertEqual(parsed["contacts"][0]["email"], "jacobjayala29@outlook.com")
        self.assertEqual(parsed["notes"], ["All home game played at the LHS Stadium"])

    def test_extract_athletic_director_contact(self):
        parsed = livingston._extract_athletic_director(ATHLETIC_DIRECTOR_TEXT)  # noqa: SLF001

        self.assertEqual(
            parsed,
            {
                "name": "Scott Winton",
                "role": "Athletic Director",
                "phone": "209-325-2665",
            },
        )


if __name__ == "__main__":
    unittest.main()
