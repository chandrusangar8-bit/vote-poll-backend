import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;
const allowedOrigins = (process.env.CLIENT_URL || '').split(',').map((origin) => origin.trim()).filter(Boolean);

app.use(cors({
  origin: allowedOrigins.length ? allowedOrigins : true
}));
app.use(express.json());

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', message: 'Backend is running' });
});

app.get('/api/polls', (req, res) => {
  res.json({
    polls: [
      { id: 1, question: 'Favorite frontend framework?', options: ['React', 'Vue', 'Angular'] },
      { id: 2, question: 'Best time to meet?', options: ['Morning', 'Afternoon', 'Evening'] }
    ]
  });
});

app.post('/api/polls', (req, res) => {
  const { question, options } = req.body;

  if (!question || !Array.isArray(options) || options.length < 2) {
    return res.status(400).json({ error: 'Question and at least 2 options are required.' });
  }

  res.status(201).json({
    message: 'Poll created successfully',
    poll: { id: Date.now(), question, options }
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Backend running on port ${PORT}`);
});
