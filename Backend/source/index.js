import express, { Request, Response } from 'express';
import cors from 'cors';
import { Pool } from 'pg';

// Initialize Express App
const app = express();
const PORT =3000;

app.use(cors());
app.use(express.json());
// PostgreSQL Connection Pool
const pool = new Pool({
    host: 'postgresql',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: 'postgres'
});



// GET Request to Fetch all Cards
app.get('/cards', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM cards LIMIT 102');
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching cards:', error);
        res.status(500).send('Internal Server Error');
    }
});

// GET Request to Fetch a Card by Name
app.get('/cards/:name', async (req, res) => {
    const { name } = req.params;
    try {
        const result = await pool.query('SELECT * FROM cards WHERE name ILIKE $1 LIMIT 102', [`%${name}%`]);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching card:', error);
        res.status(500).send('Internal Server Error');
    }
});

// Start the Server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});