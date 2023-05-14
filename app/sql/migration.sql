-- Making copy of zno records
DROP TABLE IF EXISTS students CASCADE;
CREATE TABLE students AS TABLE zno_records;
-- Adding primary key
ALTER TABLE students
ADD PRIMARY KEY (student_id);
-- Table creation
DROP TABLE IF EXISTS zno_locations CASCADE;
DROP TABLE IF EXISTS schools CASCADE;
DROP TABLE IF EXISTS results CASCADE;
CREATE TABLE zno_locations (
    location_id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    ptregname VARCHAR(256),
    ptareaname VARCHAR(256),
    pttername VARCHAR(256),
    tertypename VARCHAR(256)
);
CREATE TABLE schools (
    school_id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    location_id INT,
    eoname VARCHAR(256),
    eotypename VARCHAR(256),
    eoparent VARCHAR(256),
    FOREIGN KEY (location_id) REFERENCES zno_locations(location_id)
);
CREATE TABLE results (
    student_id INT NOT NULL,
    school_id INT,
    testname VARCHAR(256),
    teststatus VARCHAR(256),
    ball100 NUMERIC,
    ball12 NUMERIC,
    ball NUMERIC,
    lang VARCHAR(256),
    dpa_level VARCHAR(256),
    adapt_scale INT,
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (school_id) REFERENCES schools(school_id)
);
-- !!! MANAGE LINES AND CHANGE NAME OF TABLES COLUMNS
-- Add location data to zno_locations table from init table
INSERT INTO zno_locations (ptregname, ptareaname, pttername, tertypename)
SELECT DISTINCT regname,
    areaname,
    tername,
    tertypename
FROM students;
-- Add new column to students table and create FK
ALTER TABLE students
ADD COLUMN reg_location_id INT NULL,
    ADD CONSTRAINT fk_reg_location_id FOREIGN KEY (reg_location_id) REFERENCES zno_locations(location_id);
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.eoregname,
    t.eoareaname,
    t.eotername
FROM (
        SELECT DISTINCT eoregname,
            eoareaname,
            eotername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.eoregname = zno_locations.ptregname
    AND t.eoareaname = zno_locations.ptareaname
    AND t.eotername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.eoregname <> 'NaN'
    AND t.eoareaname <> 'NaN'
    AND t.eotername <> 'NaN';
-- Add data about locations from places of zno
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.ukrptregname,
    t.ukrptareaname,
    t.ukrpttername
FROM (
        SELECT DISTINCT ukrptregname,
            ukrptareaname,
            ukrpttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.ukrptregname = zno_locations.ptregname
    AND t.ukrptareaname = zno_locations.ptareaname
    AND t.ukrpttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.ukrptregname <> 'NaN'
    AND t.ukrptareaname <> 'NaN'
    AND t.ukrpttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.histptregname,
    t.histptareaname,
    t.histpttername
FROM (
        SELECT DISTINCT histptregname,
            histptareaname,
            histpttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.histptregname = zno_locations.ptregname
    AND t.histptareaname = zno_locations.ptareaname
    AND t.histpttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.histptregname <> 'NaN'
    AND t.histptareaname <> 'NaN'
    AND t.histpttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.mathptregname,
    t.mathptareaname,
    t.mathpttername
FROM (
        SELECT DISTINCT mathptregname,
            mathptareaname,
            mathpttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.mathptregname = zno_locations.ptregname
    AND t.mathptareaname = zno_locations.ptareaname
    AND t.mathpttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.mathptregname <> 'NaN'
    AND t.mathptareaname <> 'NaN'
    AND t.mathpttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.physptregname,
    t.physptareaname,
    t.physpttername
FROM (
        SELECT DISTINCT physptregname,
            physptareaname,
            physpttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.physptregname = zno_locations.ptregname
    AND t.physptareaname = zno_locations.ptareaname
    AND t.physpttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.physptregname <> 'NaN'
    AND t.physptareaname <> 'NaN'
    AND t.physpttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.chemptregname,
    t.chemptareaname,
    t.chempttername
FROM (
        SELECT DISTINCT chemptregname,
            chemptareaname,
            chempttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.chemptregname = zno_locations.ptregname
    AND t.chemptareaname = zno_locations.ptareaname
    AND t.chempttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.chemptregname <> 'NaN'
    AND t.chemptareaname <> 'NaN'
    AND t.chempttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.bioptregname,
    t.bioptareaname,
    t.biopttername
FROM (
        SELECT DISTINCT bioptregname,
            bioptareaname,
            biopttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.bioptregname = zno_locations.ptregname
    AND t.bioptareaname = zno_locations.ptareaname
    AND t.biopttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.bioptregname <> 'NaN'
    AND t.bioptareaname <> 'NaN'
    AND t.biopttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.geoptregname,
    t.geoptareaname,
    t.geopttername
FROM (
        SELECT DISTINCT geoptregname,
            geoptareaname,
            geopttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.geoptregname = zno_locations.ptregname
    AND t.geoptareaname = zno_locations.ptareaname
    AND t.geopttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.geoptregname <> 'NaN'
    AND t.geoptareaname <> 'NaN'
    AND t.geopttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.engptregname,
    t.engptareaname,
    t.engpttername
FROM (
        SELECT DISTINCT engptregname,
            engptareaname,
            engpttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.engptregname = zno_locations.ptregname
    AND t.engptareaname = zno_locations.ptareaname
    AND t.engpttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.engptregname <> 'NaN'
    AND t.engptareaname <> 'NaN'
    AND t.engpttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.fraptregname,
    t.fraptareaname,
    t.frapttername
FROM (
        SELECT DISTINCT fraptregname,
            fraptareaname,
            frapttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.fraptregname = zno_locations.ptregname
    AND t.fraptareaname = zno_locations.ptareaname
    AND t.frapttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.fraptregname <> 'NaN'
    AND t.fraptareaname <> 'NaN'
    AND t.frapttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.deuptregname,
    t.deuptareaname,
    t.deupttername
FROM (
        SELECT DISTINCT deuptregname,
            deuptareaname,
            deupttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.deuptregname = zno_locations.ptregname
    AND t.deuptareaname = zno_locations.ptareaname
    AND t.deupttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.deuptregname <> 'NaN'
    AND t.deuptareaname <> 'NaN'
    AND t.deupttername <> 'NaN';
INSERT INTO zno_locations (ptregname, ptareaname, pttername)
SELECT t.spaptregname,
    t.spaptareaname,
    t.spapttername
FROM (
        SELECT DISTINCT spaptregname,
            spaptareaname,
            spapttername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.spaptregname = zno_locations.ptregname
    AND t.spaptareaname = zno_locations.ptareaname
    AND t.spapttername = zno_locations.pttername
WHERE location_id IS NULL
    AND t.spaptregname <> 'NaN'
    AND t.spaptareaname <> 'NaN'
    AND t.spapttername <> 'NaN';
-- Add data into schools table from init table
INSERT INTO schools (location_id, eoname, eotypename, eoparent)
SELECT zno_locations.location_id,
    t.eoname,
    t.eotypename,
    t.eoparent
FROM (
        SELECT DISTINCT eoname,
            eotypename,
            eoparent,
            eoregname,
            eoareaname,
            eotername
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.eoregname = zno_locations.ptregname
    AND t.eoareaname = zno_locations.ptareaname
    AND t.eotername = zno_locations.pttername
WHERE location_id IS NOT NULL;
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.ukrptname
FROM (
        SELECT zno_locations.location_id,
            t1.ukrptname
        FROM (
                SELECT DISTINCT ukrptname,
                    ukrptregname,
                    ukrptareaname,
                    ukrpttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.ukrptregname = zno_locations.ptregname
            AND t1.ukrptareaname = zno_locations.ptareaname
            AND t1.ukrpttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.ukrptname = schools.eoname
WHERE school_id IS NULL
    AND t2.ukrptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.histptname
FROM (
        SELECT zno_locations.location_id,
            t1.histptname
        FROM (
                SELECT DISTINCT histptname,
                    histptregname,
                    histptareaname,
                    histpttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.histptregname = zno_locations.ptregname
            AND t1.histptareaname = zno_locations.ptareaname
            AND t1.histpttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.histptname = schools.eoname
WHERE school_id IS NULL
    AND t2.histptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.mathptname
FROM (
        SELECT zno_locations.location_id,
            t1.mathptname
        FROM (
                SELECT DISTINCT mathptname,
                    mathptregname,
                    mathptareaname,
                    mathpttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.mathptregname = zno_locations.ptregname
            AND t1.mathptareaname = zno_locations.ptareaname
            AND t1.mathpttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.mathptname = schools.eoname
WHERE school_id IS NULL
    AND t2.mathptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.physptname
FROM (
        SELECT zno_locations.location_id,
            t1.physptname
        FROM (
                SELECT DISTINCT physptname,
                    physptregname,
                    physptareaname,
                    physpttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.physptregname = zno_locations.ptregname
            AND t1.physptareaname = zno_locations.ptareaname
            AND t1.physpttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.physptname = schools.eoname
WHERE school_id IS NULL
    AND t2.physptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.chemptname
FROM (
        SELECT zno_locations.location_id,
            t1.chemptname
        FROM (
                SELECT DISTINCT chemptname,
                    chemptregname,
                    chemptareaname,
                    chempttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.chemptregname = zno_locations.ptregname
            AND t1.chemptareaname = zno_locations.ptareaname
            AND t1.chempttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.chemptname = schools.eoname
WHERE school_id IS NULL
    AND t2.chemptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.bioptname
FROM (
        SELECT zno_locations.location_id,
            t1.bioptname
        FROM (
                SELECT DISTINCT bioptname,
                    bioptregname,
                    bioptareaname,
                    biopttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.bioptregname = zno_locations.ptregname
            AND t1.bioptareaname = zno_locations.ptareaname
            AND t1.biopttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.bioptname = schools.eoname
WHERE school_id IS NULL
    AND t2.bioptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.geoptname
FROM (
        SELECT zno_locations.location_id,
            t1.geoptname
        FROM (
                SELECT DISTINCT geoptname,
                    geoptregname,
                    geoptareaname,
                    geopttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.geoptregname = zno_locations.ptregname
            AND t1.geoptareaname = zno_locations.ptareaname
            AND t1.geopttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.geoptname = schools.eoname
WHERE school_id IS NULL
    AND t2.geoptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.engptname
FROM (
        SELECT zno_locations.location_id,
            t1.engptname
        FROM (
                SELECT DISTINCT engptname,
                    engptregname,
                    engptareaname,
                    engpttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.engptregname = zno_locations.ptregname
            AND t1.engptareaname = zno_locations.ptareaname
            AND t1.engpttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.engptname = schools.eoname
WHERE school_id IS NULL
    AND t2.engptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.fraptname
FROM (
        SELECT zno_locations.location_id,
            t1.fraptname
        FROM (
                SELECT DISTINCT fraptname,
                    fraptregname,
                    fraptareaname,
                    frapttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.fraptregname = zno_locations.ptregname
            AND t1.fraptareaname = zno_locations.ptareaname
            AND t1.frapttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.fraptname = schools.eoname
WHERE school_id IS NULL
    AND t2.fraptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.deuptname
FROM (
        SELECT zno_locations.location_id,
            t1.deuptname
        FROM (
                SELECT DISTINCT deuptname,
                    deuptregname,
                    deuptareaname,
                    deupttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.deuptregname = zno_locations.ptregname
            AND t1.deuptareaname = zno_locations.ptareaname
            AND t1.deupttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.deuptname = schools.eoname
WHERE school_id IS NULL
    AND t2.deuptname <> 'NaN';
INSERT INTO schools (location_id, eoname)
SELECT t2.location_id,
    t2.spaptname
FROM (
        SELECT zno_locations.location_id,
            t1.spaptname
        FROM (
                SELECT DISTINCT spaptname,
                    spaptregname,
                    spaptareaname,
                    spapttername
                FROM students
            ) AS t1
            LEFT JOIN zno_locations ON t1.spaptregname = zno_locations.ptregname
            AND t1.spaptareaname = zno_locations.ptareaname
            AND t1.spapttername = zno_locations.pttername
        WHERE location_id IS NOT NULL
    ) as t2
    LEFT JOIN schools ON t2.location_id = schools.location_id
    AND t2.spaptname = schools.eoname
WHERE school_id IS NULL
    AND t2.spaptname <> 'NaN';
-- Remove more than 1 occurences
DELETE FROM schools
WHERE school_id IN (
        SELECT school_id
        FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY location_id, eoname
                        ORDER BY LENGTH(eoparent),
                            LENGTH(eotypename) DESC
                    ) AS r
                FROM schools
            ) AS t
        WHERE t.r != 1
    );
-- Add new column to students table and create FK
ALTER TABLE students
ADD COLUMN am_school_id INT NULL,
    ADD CONSTRAINT fk_am_school_id FOREIGN KEY (am_school_id) REFERENCES schools(school_id);
-- Create view for school - location relation
CREATE VIEW school_location AS
SELECT school_id,
    eoname,
    ptregname,
    ptareaname,
    pttername
FROM schools AS s
    LEFT JOIN zno_locations AS l ON s.location_id = l.location_id;
-- Addding subjects to result table
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        dpa_level
    )
SELECT student_id,
    school_id,
    spatest,
    spateststatus,
    spaball100,
    spaball12,
    spaball,
    spadpalevel
FROM (
        SELECT student_id,
            spatest,
            spateststatus,
            spaball100,
            spaball12,
            spaball,
            spadpalevel,
            spaptname,
            spaptregname,
            spaptareaname,
            spapttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.spaptname = t2.eoname
    AND t1.spaptregname = t2.ptregname
    AND t1.spaptareaname = t2.ptareaname
    AND t1.spapttername = t2.pttername
WHERE spatest <> 'NaN';
-- Remove unnessesary columns from students table
ALTER TABLE students DROP COLUMN spatest,
    DROP COLUMN spadpalevel,
    DROP COLUMN spateststatus,
    DROP COLUMN spaball100,
    DROP COLUMN spaball12,
    DROP COLUMN spaball,
    DROP COLUMN spaptname,
    DROP COLUMN spaptregname,
    DROP COLUMN spaptareaname,
    DROP COLUMN spapttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        dpa_level
    )
SELECT student_id,
    school_id,
    deutest,
    deuteststatus,
    deuball100,
    deuball12,
    deuball,
    deudpalevel
FROM (
        SELECT student_id,
            deutest,
            deuteststatus,
            deuball100,
            deuball12,
            deuball,
            deudpalevel,
            deuptname,
            deuptregname,
            deuptareaname,
            deupttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.deuptname = t2.eoname
    AND t1.deuptregname = t2.ptregname
    AND t1.deuptareaname = t2.ptareaname
    AND t1.deupttername = t2.pttername
WHERE deutest <> 'NaN';
ALTER TABLE students DROP COLUMN deutest,
    DROP COLUMN deudpalevel,
    DROP COLUMN deuteststatus,
    DROP COLUMN deuball100,
    DROP COLUMN deuball12,
    DROP COLUMN deuball,
    DROP COLUMN deuptname,
    DROP COLUMN deuptregname,
    DROP COLUMN deuptareaname,
    DROP COLUMN deupttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        dpa_level
    )
SELECT student_id,
    school_id,
    fratest,
    frateststatus,
    fraball100,
    fraball12,
    fraball,
    fradpalevel
FROM (
        SELECT student_id,
            fratest,
            frateststatus,
            fraball100,
            fraball12,
            fraball,
            fradpalevel,
            fraptname,
            fraptregname,
            fraptareaname,
            frapttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.fraptname = t2.eoname
    AND t1.fraptregname = t2.ptregname
    AND t1.fraptareaname = t2.ptareaname
    AND t1.frapttername = t2.pttername
WHERE fratest <> 'NaN';
ALTER TABLE students DROP COLUMN fratest,
    DROP COLUMN fradpalevel,
    DROP COLUMN frateststatus,
    DROP COLUMN fraball100,
    DROP COLUMN fraball12,
    DROP COLUMN fraball,
    DROP COLUMN fraptname,
    DROP COLUMN fraptregname,
    DROP COLUMN fraptareaname,
    DROP COLUMN frapttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        lang
    )
SELECT student_id,
    school_id,
    chemtest,
    chemteststatus,
    chemball100,
    chemball12,
    chemball,
    chemlang
FROM (
        SELECT student_id,
            chemtest,
            chemteststatus,
            chemball100,
            chemball12,
            chemball,
            chemlang,
            chemptname,
            chemptregname,
            chemptareaname,
            chempttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.chemptname = t2.eoname
    AND t1.chemptregname = t2.ptregname
    AND t1.chemptareaname = t2.ptareaname
    AND t1.chempttername = t2.pttername
WHERE chemtest <> 'NaN';
ALTER TABLE students DROP COLUMN chemtest,
    DROP COLUMN chemlang,
    DROP COLUMN chemteststatus,
    DROP COLUMN chemball100,
    DROP COLUMN chemball12,
    DROP COLUMN chemball,
    DROP COLUMN chemptname,
    DROP COLUMN chemptregname,
    DROP COLUMN chemptareaname,
    DROP COLUMN chempttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        lang
    )
SELECT student_id,
    school_id,
    phystest,
    physteststatus,
    physball100,
    physball12,
    physball,
    physlang
FROM (
        SELECT student_id,
            phystest,
            physteststatus,
            physball100,
            physball12,
            physball,
            physlang,
            physptname,
            physptregname,
            physptareaname,
            physpttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.physptname = t2.eoname
    AND t1.physptregname = t2.ptregname
    AND t1.physptareaname = t2.ptareaname
    AND t1.physpttername = t2.pttername
WHERE phystest <> 'NaN';
ALTER TABLE students DROP COLUMN phystest,
    DROP COLUMN physlang,
    DROP COLUMN physteststatus,
    DROP COLUMN physball100,
    DROP COLUMN physball12,
    DROP COLUMN physball,
    DROP COLUMN physptname,
    DROP COLUMN physptregname,
    DROP COLUMN physptareaname,
    DROP COLUMN physpttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        lang
    )
SELECT student_id,
    school_id,
    geotest,
    geoteststatus,
    geoball100,
    geoball12,
    geoball,
    geolang
FROM (
        SELECT student_id,
            geotest,
            geoteststatus,
            geoball100,
            geoball12,
            geoball,
            geolang,
            geoptname,
            geoptregname,
            geoptareaname,
            geopttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.geoptname = t2.eoname
    AND t1.geoptregname = t2.ptregname
    AND t1.geoptareaname = t2.ptareaname
    AND t1.geopttername = t2.pttername
WHERE geotest <> 'NaN';
ALTER TABLE students DROP COLUMN geotest,
    DROP COLUMN geolang,
    DROP COLUMN geoteststatus,
    DROP COLUMN geoball100,
    DROP COLUMN geoball12,
    DROP COLUMN geoball,
    DROP COLUMN geoptname,
    DROP COLUMN geoptregname,
    DROP COLUMN geoptareaname,
    DROP COLUMN geopttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        lang
    )
SELECT student_id,
    school_id,
    biotest,
    bioteststatus,
    bioball100,
    bioball12,
    bioball,
    biolang
FROM (
        SELECT student_id,
            biotest,
            bioteststatus,
            bioball100,
            bioball12,
            bioball,
            biolang,
            bioptname,
            bioptregname,
            bioptareaname,
            biopttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.bioptname = t2.eoname
    AND t1.bioptregname = t2.ptregname
    AND t1.bioptareaname = t2.ptareaname
    AND t1.biopttername = t2.pttername
WHERE biotest <> 'NaN';
ALTER TABLE students DROP COLUMN biotest,
    DROP COLUMN biolang,
    DROP COLUMN bioteststatus,
    DROP COLUMN bioball100,
    DROP COLUMN bioball12,
    DROP COLUMN bioball,
    DROP COLUMN bioptname,
    DROP COLUMN bioptregname,
    DROP COLUMN bioptareaname,
    DROP COLUMN biopttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        dpa_level
    )
SELECT student_id,
    school_id,
    engtest,
    engteststatus,
    engball100,
    engball12,
    engball,
    engdpalevel
FROM (
        SELECT student_id,
            engtest,
            engteststatus,
            engball100,
            engball12,
            engball,
            engdpalevel,
            engptname,
            engptregname,
            engptareaname,
            engpttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.engptname = t2.eoname
    AND t1.engptregname = t2.ptregname
    AND t1.engptareaname = t2.ptareaname
    AND t1.engpttername = t2.pttername
WHERE engtest <> 'NaN';
ALTER TABLE students DROP COLUMN engtest,
    DROP COLUMN engdpalevel,
    DROP COLUMN engteststatus,
    DROP COLUMN engball100,
    DROP COLUMN engball12,
    DROP COLUMN engball,
    DROP COLUMN engptname,
    DROP COLUMN engptregname,
    DROP COLUMN engptareaname,
    DROP COLUMN engpttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        lang
    )
SELECT student_id,
    school_id,
    mathtest,
    mathteststatus,
    mathball100,
    mathball12,
    mathball,
    mathlang
FROM (
        SELECT student_id,
            mathtest,
            mathteststatus,
            mathball100,
            mathball12,
            mathball,
            mathlang,
            mathptname,
            mathptregname,
            mathptareaname,
            mathpttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.mathptname = t2.eoname
    AND t1.mathptregname = t2.ptregname
    AND t1.mathptareaname = t2.ptareaname
    AND t1.mathpttername = t2.pttername
WHERE mathtest <> 'NaN';
ALTER TABLE students DROP COLUMN mathtest,
    DROP COLUMN mathlang,
    DROP COLUMN mathteststatus,
    DROP COLUMN mathball100,
    DROP COLUMN mathball12,
    DROP COLUMN mathball,
    DROP COLUMN mathptname,
    DROP COLUMN mathptregname,
    DROP COLUMN mathptareaname,
    DROP COLUMN mathpttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        lang
    )
SELECT student_id,
    school_id,
    histtest,
    histteststatus,
    histball100,
    histball12,
    histball,
    histlang
FROM (
        SELECT student_id,
            histtest,
            histteststatus,
            histball100,
            histball12,
            histball,
            histlang,
            histptname,
            histptregname,
            histptareaname,
            histpttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.histptname = t2.eoname
    AND t1.histptregname = t2.ptregname
    AND t1.histptareaname = t2.ptareaname
    AND t1.histpttername = t2.pttername
WHERE histtest <> 'NaN';
ALTER TABLE students DROP COLUMN histtest,
    DROP COLUMN histlang,
    DROP COLUMN histteststatus,
    DROP COLUMN histball100,
    DROP COLUMN histball12,
    DROP COLUMN histball,
    DROP COLUMN histptname,
    DROP COLUMN histptregname,
    DROP COLUMN histptareaname,
    DROP COLUMN histpttername;
INSERT INTO results (
        student_id,
        school_id,
        testname,
        teststatus,
        ball100,
        ball12,
        ball,
        adapt_scale
    )
SELECT student_id,
    school_id,
    ukrtest,
    ukrteststatus,
    ukrball100,
    ukrball12,
    ukrball,
    ukradaptscale
FROM (
        SELECT student_id,
            ukrtest,
            ukrteststatus,
            ukrball100,
            ukrball12,
            ukrball,
            ukradaptscale,
            ukrptname,
            ukrptregname,
            ukrptareaname,
            ukrpttername
        FROM students
    ) AS t1
    LEFT JOIN school_location AS t2 ON t1.ukrptname = t2.eoname
    AND t1.ukrptregname = t2.ptregname
    AND t1.ukrptareaname = t2.ptareaname
    AND t1.ukrpttername = t2.pttername
WHERE ukrtest <> 'NaN';
ALTER TABLE students DROP COLUMN ukrtest,
    DROP COLUMN ukrteststatus,
    DROP COLUMN ukrball100,
    DROP COLUMN ukrball12,
    DROP COLUMN ukrball,
    DROP COLUMN ukradaptscale,
    DROP COLUMN ukrptname,
    DROP COLUMN ukrptregname,
    DROP COLUMN ukrptareaname,
    DROP COLUMN ukrpttername;
-- Remove view for school - location relation
DROP VIEW school_location;
-- Add values to new column into students table
UPDATE students
SET reg_location_id = zno_locations.location_id
FROM (
        SELECT student_id,
            regname,
            areaname,
            tername,
            tertypename
        FROM students
    ) AS t
    LEFT JOIN zno_locations ON t.regname = zno_locations.ptregname
    AND t.areaname = zno_locations.ptareaname
    AND t.tername = zno_locations.pttername
    AND t.tertypename = zno_locations.tertypename
WHERE students.student_id = t.student_id;
ALTER TABLE students DROP COLUMN regname,
    DROP COLUMN areaname,
    DROP COLUMN tername,
    DROP COLUMN tertypename;
-- Add values to new column in students table
UPDATE students
SET am_school_id = t2.school_id
FROM (
        SELECT student_id,
            eoname,
            eotypename,
            eoparent,
            eoregname,
            eoareaname,
            eotername
        FROM students
    ) AS t1
    LEFT JOIN (
        SELECT school_id,
            eoname,
            eotypename,
            eoparent,
            ptregname,
            ptareaname,
            pttername
        FROM schools AS s
            LEFT JOIN zno_locations AS l on s.location_id = l.location_id
    ) as t2 ON t1.eoname = t2.eoname
    AND t1.eotypename = t2.eotypename
    AND t1.eoparent = t2.eoparent
    AND t1.eoregname = t2.ptregname
    AND t1.eoareaname = t2.ptareaname
    AND t1.eotername = t2.pttername
WHERE students.student_id = t1.student_id;
-- Remove columns which was replaced by new column in students table
ALTER TABLE students DROP COLUMN eoname,
    DROP COLUMN eotypename,
    DROP COLUMN eoparent,
    DROP COLUMN eoregname,
    DROP COLUMN eoareaname,
    DROP COLUMN eotername;