const fs = require('fs');
const express = require('express');
const dotenv = require('dotenv');
const { Pool } = require('pg');
const parse = require('csv-parse');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// PostgreSQL configuration
const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: 5432,
});

// Parse CSV file and insert data into PostgreSQL
const processCSV = () => {
  const csvFilePath = process.env.CSV_FILE_PATH;

  fs.readFile(csvFilePath, (err, fileData) => {
    if (err) {
      console.error('Error reading CSV file:', err);
      return;
    }

    parse(fileData, { columns: true, trim: true }, (parseErr, rows) => {
      if (parseErr) {
        console.error('Error parsing CSV:', parseErr);
        return;
      }

      const insertQueries = rows.map(row => {
        const { name, age, ...additionalInfo } = row;
        const [firstName, lastName] = name.split(' ');

        const query = {
          text: `INSERT INTO public.users (name, age, address, additional_info) VALUES ($1, $2, $3, $4)`,
          values: [
            `${firstName} ${lastName}`,
            parseInt(age),
            JSON.stringify({}),
            JSON.stringify(additionalInfo),
          ],
        };

        return query;
      });

      pool.connect((connectErr, client, done) => {
        if (connectErr) {
          console.error('Error connecting to database:', connectErr);
          return;
        }

        client.query('BEGIN', (beginErr) => {
          if (beginErr) {
            console.error('Error starting transaction:', beginErr);
            done();
            return;
          }

          insertQueries.forEach(query => {
            client.query(query, (queryErr) => {
              if (queryErr) {
                console.error('Error executing query:', queryErr);
                client.query('ROLLBACK', rollbackErr => {
                  if (rollbackErr) {
                    console.error('Error rolling back transaction:', rollbackErr);
                  }
                  done();
                });
              }
            });
          });

          client.query('COMMIT', (commitErr) => {
            if (commitErr) {
              console.error('Error committing transaction:', commitErr);
            }
            done();
          });
        });
      });
    });
  });
};

// Calculate age distribution
const calculateAgeDistribution = () => {
  pool.query('SELECT age, COUNT(*) AS count FROM public.users GROUP BY age ORDER BY age', (err, result) => {
    if (err) {
      console.error('Error fetching age distribution:', err);
      return;
    }

    console.log('Age Distribution:');
    result.rows.forEach(row => {
      console.log(`${row.age} years old: ${row.count} users`);
    });
  });
};

// Process CSV file and calculate age distribution on application start
processCSV();
calculateAgeDistribution();

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
