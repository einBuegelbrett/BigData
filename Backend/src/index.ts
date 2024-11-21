import express, { Request, Response } from 'express';
import cors from 'cors';
import { Pool } from 'pg';

// Initialize Express App
const app = express();
const PORT = process.env.PORT || 3000;

// PostgreSQL Connection Pool
const pool = new Pool({
    host: 'postgresql', // Update as necessary
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: 'postgres'
});

// Enable CORS
app.use(cors());

// Middleware
app.use(express.json());

// GET Request to Fetch all Cards
app.get('/cards', async (req: Request, res: Response) => {
    try {
        const result = await pool.query('SELECT * FROM cards');
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching cards:', error);
        res.status(500).send('Internal Server Error');
    }
});

// GET Request to Fetch a Card by Name
app.get('/cards/:name', async (req: Request, res: Response) => {
    try {
        const result = await pool.query('SELECT * FROM cards', [req.params.name]);
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
