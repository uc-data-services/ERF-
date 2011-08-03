DROP TABLE IF EXISTS r_s_bridge;
DROP TABLE IF EXISTS r_t_bridge;
DROP TABLE IF EXISTS resource;
DROP TABLE IF EXISTS subject;
DROP TABLE IF EXISTS type;

CREATE TABLE "r_s_bridge" ("rid" INTEGER NOT NULL , "sid" INTEGER NOT NULL , "is_core" BOOL, PRIMARY KEY ("rid", "sid"));
CREATE TABLE "r_t_bridge" ("rid" INTEGER NOT NULL , "tid" INTEGER NOT NULL , PRIMARY KEY ("rid", "tid"));
CREATE TABLE "resource" ("rid" INTEGER PRIMARY KEY  NOT NULL, "title" TEXT,"resource_id" INTEGER,"text" TEXT DEFAULT (NULL),
                "description" TEXT,"coverage" TEXT DEFAULT (NULL), "licensing" TEXT,"last_modified" DATETIME,"url" TEXT,
                "alternative_title" TEXT DEFAULT (NULL));             
CREATE TABLE "subject" ("sid" INTEGER PRIMARY KEY  NOT NULL , "term" TEXT UNIQUE );
CREATE TABLE "type" ("tid" INTEGER PRIMARY KEY  NOT NULL , "type" TEXT UNIQUE);