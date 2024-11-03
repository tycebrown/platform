import "./worker-env";
import { query, close } from "@/shared/db";

interface DataRow {
  languageId: string;
  languageCode: string;
  bookId: number;
  verseId: string;
  phraseId: number;
  wordId: string;
  gloss: string;
  footnote?: string;
}

async function run() {
  log("starting export");
  const dataQuery = await query<DataRow>(
    /*sql*/ `
    WITH 
      data_entries AS (
        SELECT 
          l.id AS "languageId",
          l.name AS "languageCode",
          b.id AS "bookId", 
          v.id AS "verseId",
          ph.id AS "phraseId",
          w.id AS "wordId",
          ph."deletedAt" AS "phraseDeletedAt", 
          g.gloss AS "gloss", 
          g.state AS "glossState",
          ge."syncState" AS "glossSyncState",
          fn.content AS "footnote"
        FROM
          "Book" AS b CROSS JOIN "Language" AS l
          JOIN "Verse" AS v ON v."bookId" = b.id
          JOIN "Word" AS w ON w."verseId" = v.id
          LEFT JOIN "PhraseWord" AS phw ON phw."wordId" = w.id
          LEFT JOIN "Phrase" AS ph ON ph.id = phw."phraseId"
          LEFT JOIN "Gloss" AS g ON g."phraseId" = ph.id
          LEFT JOIN "GlossEvent" AS ge ON ge."phraseId" = ph.id
          LEFT JOIN "Footnote" AS fn ON fn."phraseId" = ph.id
        WHERE ph.id IS NULL OR ph."languageId" = l.id
      ),
      complete_books AS (
        SELECT "languageId", "bookId" FROM data_entries
        GROUP BY "languageId", "bookId"
        HAVING every(
          "phraseId" IS NOT NULL 
          AND "phraseDeletedAt" IS NULL
          AND "glossState" IS NOT NULL
          AND "glossState" = 'APPROVED'
        ) AND bool_or("glossSyncState" = 'PENDING')
      )
    SELECT 
      data_entries."languageId",
      data_entries."languageCode",
      data_entries."bookId", 
      data_entries."verseId",
      data_entries."phraseId",
      data_entries."wordId",
      data_entries."gloss", 
      data_entries."footnote"
    FROM complete_books JOIN data_entries USING ("languageId", "bookId")`,
    []
  );

  log("query successful; grouping data");
  const rawData = Object.groupBy(dataQuery.rows, (row) => row.languageCode);
  log("completed data gathered");

  const languageFoldersResponse = await fetch(
    `https://api.github.com/repos/tycebrown/test-data-repo/contents/`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer  ${process.env.DATA_REPO_TOKEN}`,
        Accept: "application/vnd.github+json",
        "Content-type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
    }
  );
  if (!languageFoldersResponse.ok)
    throw new Error(
      `fetching from '${languageFoldersResponse.url}': status ${languageFoldersResponse.status}`
    );
  const languageFolders = await languageFoldersResponse.json();

  const languageDataShas = await Promise.all(
    languageFolders
      .filter((languageFolder: any) =>
        Object.keys(rawData).includes(languageFolder.name)
      )
      .map(async (entry: any) => {
        const languageDataFile = await fetch(
          `https://api.github.com/repos/tycebrown/test-data-repo/contents/${entry.name}/data.json`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${process.env.DATA_REPO_TOKEN}`,
              Accept: "application/vnd.github+json",
              "Content-type": "application/json",
              "X-GitHub-Api-Version": "2022-11-28",
            },
          }
        ).then((res) => res.json());
        return { code: entry.name, sha: languageDataFile?.sha };
      })
  );

  log("fetched languages");

  log("exporting data...");
  await Promise.all(
    Object.entries(rawData).map(([dataLanguageCode, rawLanguageData]: any) =>
      fetch(
        `https://api.github.com/repos/tycebrown/test-data-repo/contents/${dataLanguageCode}/data.json`,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${process.env.DATA_REPO_TOKEN}`,
            Accept: "application/vnd.github+json",
            "Content-type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
          },
          body: JSON.stringify({
            message: `Update at ${new Date().toISOString()}`,
            content: generateDataFileContent(rawLanguageData),
            sha: languageDataShas.find(
              ({ code }: any) => dataLanguageCode === code
            )?.sha,
          }),
        }
      )
    )
  );

  log("export completed successfully");
}

function generateDataFileContent(rawLanguageData: DataRow[]) {
  /**
   * Overall structure of language data:
      book
        bookId
        verse
          verseId
          words
            wordId
            gloss
            footnote
            linkedWords
   */

  const structuredData = Object.entries(
    Object.groupBy(rawLanguageData, (row) => row.bookId)
  ).map(([bookId, rows]) => ({
    bookId,
    verses:
      rows &&
      Object.entries(Object.groupBy(rows, (row) => row.verseId)).map(
        ([verseId, rows]) => ({
          verseId,
          words:
            rows &&
            Object.values(Object.groupBy(rows, (row) => row.phraseId)).flatMap(
              (linkedRows) => {
                const wordIds = linkedRows?.map((row) => row.wordId);
                return linkedRows?.map((row) => ({
                  wordId: row.wordId,
                  gloss: row.gloss,
                  footnote: row.footnote,
                  linkedWords: wordIds?.filter(
                    (wordId) => wordId !== row.wordId
                  ),
                }));
              }
            ),
        })
      ),
  }));
  return Buffer.from(JSON.stringify(structuredData)).toString("base64");
}

function log(message: string) {
  console.log(`EXPORT (${message})`);
}

run()
  .catch((error) => log(`${error}`))
  .finally(close);
