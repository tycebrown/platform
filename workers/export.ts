import { query } from "@/shared/db";

interface BookData {
  languageId: string;
  bookId: string;
  wordIds: string[][];
  glosses: string[];
}

async function run() {
  log("starting export");
  const completeBooksData = query<BookData[]>(
    /*sql*/ `
        WITH 
        completed_books AS (
            SELECT l.id AS "languageId", b.id AS "bookId",
            COUNT(*) FILTER (WHERE ph_phw.id IS NOT NULL 
                AND ph_phw."deletedAt" IS NULL
                AND g."state" IS NOT NULL
                AND g."state" = 'APPROVED'
            ) AS "approvedCount",
            COUNT(*) AS "wordCount"
            FROM "Language" AS l 
            CROSS JOIN "Book" AS b
            JOIN "Verse" AS v ON v."bookId" = b.id
            JOIN "Word" AS w ON w."verseId" = v.id
            LEFT JOIN LATERAL (
                SELECT * FROM "Phrase" AS ph 
                JOIN "PhraseWord" AS phw ON phw."phraseId" = ph.id
                WHERE ph."languageId" = l.id
            ) AS ph_phw ON ph_phw."wordId" = w.id
            LEFT JOIN "Gloss" AS g ON g."phraseId" = ph_phw.id
            GROUP BY l.id, b.id
            HAVING every(ph_phw.id IS NOT NULL 
            AND ph_phw."deletedAt" IS NULL
            AND g."state" IS NOT NULL
            AND g."state" = 'APPROVED'
            )
        ),
        books_to_update AS (
            SELECT completed_books."languageId", completed_books."bookId"
            FROM completed_books 
            JOIN "Verse" AS v ON v."bookId" = completed_books."bookId"
            JOIN "Word" AS w ON w."verseId" = v.id
            JOIN "PhraseWord" AS phw ON phw."wordId" = w.id
            JOIN "Phrase" AS ph ON ph.id = phw."phraseId"
            JOIN "GlossEvent" AS ge ON ge."phraseId" = ph.id
            WHERE ge."syncState" = 'PENDING' AND ph."languageId" = completed_books."languageId"
            GROUP BY completed_books."languageId", completed_books."bookId"
        ),
        completed_books_data AS (
            SELECT books_to_update."languageId", books_to_update."bookId", array_agg(dat."wordIds") AS "wordIds", array_agg(dat."gloss") AS "glosses" 
            FROM books_to_update 
            JOIN "Verse" AS v ON v."bookId" = books_to_update."bookId"
            JOIN "Word" AS w ON w."verseId" = v.id
            JOIN "PhraseWord" AS phw ON phw."wordId" = w.id
            JOIN "Phrase" AS ph ON ph.id = phw."phraseId"
            JOIN (
                SELECT "Phrase".id AS "phraseId", to_jsonb(array_agg("Word".id)) AS "wordIds", (array_agg("Gloss"."gloss"))[1] AS "gloss" FROM "Phrase" 
                JOIN "PhraseWord" ON "PhraseWord"."phraseId" = "Phrase".id
                JOIN "Word" ON "PhraseWord"."wordId" = "Word".id
                JOIN "Gloss" ON "Gloss"."phraseId" = "Phrase".id
                GROUP BY "Phrase".id
            ) AS dat ON dat."phraseId" = ph.id
            WHERE ph."languageId" = books_to_update."languageId"
            GROUP BY books_to_update."languageId", books_to_update."bookId"
        )
        SELECT * FROM completed_books_data`,
    []
  );
  log("completed data gathered");

  const fetchFileResponse = await fetch(
    `https://api.github.com/repos/tycebrown/test-data-repo/contents/`,
    {
      method: "GET",
      headers: {
        Authorization: "Bearer [the token]",
        Accept: "application/vnd.github+json",
        "Content-type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
    }
  );
  log("fetched repo content");

  const crudFileResponse = await fetch(
    `https://api.github.com/repos/tycebrown/test-data-repo/contents/yes.csv`,
    {
      method: "PUT",
      headers: {
        Authorization: "Bearer [the token]",
        Accept: "application/vnd.github+json",
        "Content-type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({
        message: `Update at ${new Date().toISOString()}`,
        content: "TWVzc2FnZQpIZWxsbyBXb3JsZAo=",
        sha: (
          await fetchFileResponse.json()
        ).entries.find((entry: any) => entry.name === "yes.csv").sha,
      }),
    }
  );

  log("export completed successfully");
}

function log(message: string) {
  console.log(`EXPORT (${message})`);
}

run()
  .catch((error) => log(`${error}`))
  .finally(async () => await close());
