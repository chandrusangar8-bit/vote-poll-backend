import express from 'express';
import cors from 'cors';
import { createServer as createHttpServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'path';
import fs from 'fs';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { MongoClient, Db } from 'mongodb';
import { createServer as createViteServer } from 'vite';

const PORT = Number(process.env.PORT || 5000);
const JWT_SECRET = process.env.JWT_SECRET || 'pulsevote_jwt_secret_key_prod_2026';
const MONGODB_URI = process.env.MONGODB_URI || '';
const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME || 'pulsevote';
const DATA_DIR = path.resolve(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Interfaces
interface User {
  id: string;
  name: string;
  email: string;
  passwordHash: string;
  createdAt: string;
}

interface PollOption {
  id: string;
  text: string;
  color: string;
  count: number;
}

interface Poll {
  id: string;
  creatorId: string;
  creatorName: string;
  question: string;
  description?: string;
  category: string;
  options: PollOption[];
  isClosed: boolean;
  allowMultiple: boolean;
  totalVotes: number;
  createdAt: string;
  updatedAt: string;
  expiresAt?: string | null;
}

interface VoteRecord {
  id: string;
  pollId: string;
  voterKey: string;
  selectedOptionIds: string[];
  ip: string;
  createdAt: string;
}

interface DatabaseSchema {
  users: User[];
  polls: Poll[];
  votes: VoteRecord[];
}

// -------------------------------------------------------------
// MongoDB-like Persistent Document Store with Local JSON Backup
// -------------------------------------------------------------
class DocumentDatabase {
  private data: DatabaseSchema = {
    users: [],
    polls: [],
    votes: []
  };
  private mongoClient: MongoClient | null = null;
  private mongoDb: Db | null = null;

  constructor() {
    this.load();
  }

  public async initialize() {
    if (MONGODB_URI) {
      try {
        this.mongoClient = new MongoClient(MONGODB_URI);
        await this.mongoClient.connect();
        this.mongoDb = this.mongoClient.db(MONGODB_DB_NAME);
        console.log(`Connected to MongoDB database: ${MONGODB_DB_NAME}`);
        await this.loadFromMongo();
      } catch (err) {
        console.error('MongoDB connection failed, falling back to local JSON storage:', err);
        this.mongoClient = null;
        this.mongoDb = null;
        this.load();
      }
    } else {
      this.load();
    }

    if (this.data.users.length === 0 && this.data.polls.length === 0) {
      await this.seedInitialData();
    }
  }

  private async loadFromMongo() {
    if (!this.mongoDb) return;

    const usersCollection = this.mongoDb.collection<User>('users');
    const pollsCollection = this.mongoDb.collection<Poll>('polls');
    const votesCollection = this.mongoDb.collection<VoteRecord>('votes');

    const [users, polls, votes] = await Promise.all([
      usersCollection.find({}).toArray(),
      pollsCollection.find({}).toArray(),
      votesCollection.find({}).toArray()
    ]);

    this.data = { users, polls, votes };
  }

  private load() {
    try {
      if (fs.existsSync(DB_FILE) && !MONGODB_URI) {
        const raw = fs.readFileSync(DB_FILE, 'utf-8');
        this.data = JSON.parse(raw);
      }
    } catch (err) {
      console.error('Failed to load database from disk:', err);
    }
  }

  private async persistToMongo() {
    if (!this.mongoDb) return;

    const usersCollection = this.mongoDb.collection<User>('users');
    const pollsCollection = this.mongoDb.collection<Poll>('polls');
    const votesCollection = this.mongoDb.collection<VoteRecord>('votes');

    await Promise.all([
      usersCollection.deleteMany({}),
      pollsCollection.deleteMany({}),
      votesCollection.deleteMany({})
    ]);

    if (this.data.users.length > 0) {
      await usersCollection.insertMany(this.data.users);
    }
    if (this.data.polls.length > 0) {
      await pollsCollection.insertMany(this.data.polls);
    }
    if (this.data.votes.length > 0) {
      await votesCollection.insertMany(this.data.votes);
    }
  }

  public save() {
    if (MONGODB_URI && this.mongoDb) {
      void this.persistToMongo();
      return;
    }

    try {
      fs.writeFileSync(DB_FILE, JSON.stringify(this.data, null, 2), 'utf-8');
    } catch (err) {
      console.error('Failed to save database to disk:', err);
    }
  }

  public get users() { return this.data.users; }
  public get polls() { return this.data.polls; }
  public get votes() { return this.data.votes; }

  private async seedInitialData() {
    const demoUserId = 'usr_demo_1001';
    const salt = bcrypt.genSaltSync(10);
    const passwordHash = bcrypt.hashSync('password123', salt);

    const demoUser: User = {
      id: demoUserId,
      name: 'Alex Rivera',
      email: 'demo@pulsevote.io',
      passwordHash,
      createdAt: new Date().toISOString()
    };

    const initialPolls: Poll[] = [
      {
        id: 'poll_remote_work_policy',
        creatorId: demoUserId,
        creatorName: 'Alex Rivera',
        question: 'What is your organization’s planned workplace model for the coming fiscal year?',
        description: 'Anonymous pulse check across product and engineering teams.',
        category: 'Workplace',
        isClosed: true,
        allowMultiple: false,
        totalVotes: 215,
        createdAt: new Date(Date.now() - 3600 * 1000 * 96).toISOString(),
        updatedAt: new Date().toISOString(),
        options: [
          { id: 'opt_r1', text: '100% Fully Distributed & Async-First', color: '#2dd4bf', count: 104 },
          { id: 'opt_r2', text: 'Flexible Hybrid (2-3 Anchor Days)', color: '#60a5fa', count: 78 },
          { id: 'opt_r3', text: 'Quarterly In-Person Summits + Remote', color: '#fbbf24', count: 24 },
          { id: 'opt_r4', text: 'Mandatory Full-Time Onsite Presence', color: '#f87171', count: 9 }
        ]
      }
    ];

    this.data.users.push(demoUser);
    this.data.polls.push(...initialPolls);
    this.save();
    if (this.mongoDb) {
      await this.persistToMongo();
    }
  }
}

const db = new DocumentDatabase();
await db.initialize();

// -------------------------------------------------------------------
// Redis-Style In-Memory Atomic Counter & Pub/Sub Implementation
// -------------------------------------------------------------------
class RedisEngine {
  // Atomic counters: Map<pollId, Map<optionId, number>>
  private counters: Map<string, Map<string, number>> = new Map();
  // PubSub listeners: Map<channel, Set<(message: any) => void>>
  private subscribers: Map<string, Set<(message: any) => void>> = new Map();

  constructor() {
    this.initFromDatabase();
  }

  public initFromDatabase() {
    for (const poll of db.polls) {
      const optionMap = new Map<string, number>();
      for (const opt of poll.options) {
        optionMap.set(opt.id, opt.count);
      }
      this.counters.set(poll.id, optionMap);
    }
  }

  public registerPoll(poll: Poll) {
    const optionMap = new Map<string, number>();
    for (const opt of poll.options) {
      optionMap.set(opt.id, opt.count);
    }
    this.counters.set(poll.id, optionMap);
  }

  public removePoll(pollId: string) {
    this.counters.delete(pollId);
    this.subscribers.delete(`poll:${pollId}`);
  }

  // Atomic INCR operation
  public atomicIncr(pollId: string, optionIds: string[]): { [optionId: string]: number } {
    let optionMap = this.counters.get(pollId);
    if (!optionMap) {
      optionMap = new Map();
      this.counters.set(pollId, optionMap);
    }

    for (const optId of optionIds) {
      const current = optionMap.get(optId) || 0;
      optionMap.set(optId, current + 1);
    }

    const result: { [optionId: string]: number } = {};
    for (const [key, val] of optionMap.entries()) {
      result[key] = val;
    }
    return result;
  }

  public getCounts(pollId: string): { [optionId: string]: number } {
    const optionMap = this.counters.get(pollId);
    const result: { [optionId: string]: number } = {};
    if (optionMap) {
      for (const [key, val] of optionMap.entries()) {
        result[key] = val;
      }
    }
    return result;
  }

  // Redis Pub/Sub: Subscribe
  public subscribe(channel: string, listener: (message: any) => void) {
    if (!this.subscribers.has(channel)) {
      this.subscribers.set(channel, new Set());
    }
    this.subscribers.get(channel)!.add(listener);
    return () => {
      this.subscribers.get(channel)?.delete(listener);
    };
  }

  // Redis Pub/Sub: Publish
  public publish(channel: string, message: any) {
    const listeners = this.subscribers.get(channel);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(message);
        } catch (e) {
          console.error('Error delivering Redis pub/sub message:', e);
        }
      }
    }
  }
}

const redis = new RedisEngine();

// -------------------------------------------------------------
// Express App & WebSocket Setup
// -------------------------------------------------------------
const app = express();
app.use(cors({ origin: process.env.CLIENT_URL || true }));
app.use(express.json());

app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Live Vote backend is running' });
});

// Helper: voter key calculation for strict 1-person-1-vote
function calculateVoterKey(req: express.Request, pollId: string, userId?: string): string {
  if (userId) {
    return `user:${userId}:${pollId}`;
  }
  const clientFingerprint = (req.headers['x-voter-token'] as string) || (req.body && req.body.voterToken) || '';
  const forwarded = req.headers['x-forwarded-for'];
  const ip = typeof forwarded === 'string' ? forwarded.split(',')[0].trim() : req.socket.remoteAddress || 'unknown-ip';
  return `anon:${pollId}:${clientFingerprint || ip}`;
}

// Authentication Middleware
function authenticateToken(req: express.Request, res: express.Response, next: express.NextFunction) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  jwt.verify(token, JWT_SECRET, (err, decoded: any) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    (req as any).user = decoded;
    next();
  });
}

// Optional Auth (for voter identity check if token present)
function optionalAuthenticate(req: express.Request, res: express.Response, next: express.NextFunction) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (token) {
    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      (req as any).user = decoded;
    } catch {
      // Ignore invalid optional token
    }
  }
  next();
}

// -------------------------------------------------------------
// Auth Routes
// -------------------------------------------------------------
app.post('/api/auth/register', (req, res) => {
  const { name, email, password } = req.body;
  if (!name || !email || !password) {
    return res.status(400).json({ error: 'Name, email, and password are required' });
  }

  const normalizedEmail = email.toLowerCase().trim();
  const existing = db.users.find(u => u.email.toLowerCase() === normalizedEmail);
  if (existing) {
    return res.status(409).json({ error: 'An account with this email already exists' });
  }

  const salt = bcrypt.genSaltSync(10);
  const passwordHash = bcrypt.hashSync(password, salt);
  const newUser: User = {
    id: `usr_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
    name: name.trim(),
    email: normalizedEmail,
    passwordHash,
    createdAt: new Date().toISOString()
  };

  db.users.push(newUser);
  db.save();

  const token = jwt.sign({ id: newUser.id, name: newUser.name, email: newUser.email }, JWT_SECRET, { expiresIn: '7d' });
  res.status(201).json({
    token,
    user: { id: newUser.id, name: newUser.name, email: newUser.email }
  });
});

app.post('/api/auth/login', (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Email and password are required' });
  }

  const normalizedEmail = email.toLowerCase().trim();
  const user = db.users.find(u => u.email.toLowerCase() === normalizedEmail);
  if (!user) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }

  const match = bcrypt.compareSync(password, user.passwordHash);
  if (!match) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }

  const token = jwt.sign({ id: user.id, name: user.name, email: user.email }, JWT_SECRET, { expiresIn: '7d' });
  res.json({
    token,
    user: { id: user.id, name: user.name, email: user.email }
  });
});

app.get('/api/auth/me', authenticateToken, (req, res) => {
  const userPayload = (req as any).user;
  const user = db.users.find(u => u.id === userPayload.id);
  if (!user) {
    return res.status(404).json({ error: 'User not found' });
  }
  res.json({
    user: { id: user.id, name: user.name, email: user.email }
  });
});

// -------------------------------------------------------------
// Creator Dashboard & Management APIs (Auth Required)
// -------------------------------------------------------------
app.get('/api/creator/polls', authenticateToken, (req, res) => {
  const userId = (req as any).user.id;
  const userPolls = db.polls
    .filter(p => p.creatorId === userId)
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

  // Attach latest atomic counts
  const enriched = userPolls.map(poll => {
    const liveCounts = redis.getCounts(poll.id);
    let total = 0;
    const optionsWithCounts = poll.options.map(opt => {
      const count = liveCounts[opt.id] !== undefined ? liveCounts[opt.id] : opt.count;
      total += count;
      return { ...opt, count };
    });
    return {
      ...poll,
      options: optionsWithCounts,
      totalVotes: total
    };
  });

  res.json({ polls: enriched });
});

app.get('/api/creator/stats', authenticateToken, (req, res) => {
  const userId = (req as any).user.id;
  const userPolls = db.polls.filter(p => p.creatorId === userId);
  
  const totalPolls = userPolls.length;
  const activePolls = userPolls.filter(p => !p.isClosed).length;
  
  let totalVotes = 0;
  let mostActivePoll: { id: string; question: string; votes: number } | null = null;

  for (const poll of userPolls) {
    const liveCounts = redis.getCounts(poll.id);
    let pollVotes = 0;
    for (const opt of poll.options) {
      pollVotes += liveCounts[opt.id] !== undefined ? liveCounts[opt.id] : opt.count;
    }
    totalVotes += pollVotes;
    if (!mostActivePoll || pollVotes > mostActivePoll.votes) {
      mostActivePoll = { id: poll.id, question: poll.question, votes: pollVotes };
    }
  }

  const avgVotesPerPoll = totalPolls > 0 ? Math.round(totalVotes / totalPolls) : 0;

  res.json({
    totalPolls,
    activePolls,
    closedPolls: totalPolls - activePolls,
    totalVotes,
    avgVotesPerPoll,
    mostActivePoll
  });
});

app.post('/api/polls', authenticateToken, (req, res) => {
  const user = (req as any).user;
  const { question, description, category, options, allowMultiple, expiresAt } = req.body;

  if (!question || typeof question !== 'string' || question.trim().length < 4) {
    return res.status(400).json({ error: 'Question must be at least 4 characters long' });
  }

  if (!Array.isArray(options) || options.length < 2) {
    return res.status(400).json({ error: 'A poll must have at least 2 options' });
  }

  const defaultColors = ['#38bdf8', '#818cf8', '#34d399', '#f472b6', '#fbbf24', '#a78bfa', '#f87171', '#2dd4bf'];
  const formattedOptions: PollOption[] = options.map((opt: any, index: number) => {
    const text = typeof opt === 'string' ? opt : opt.text;
    const color = (typeof opt === 'object' && opt.color) ? opt.color : defaultColors[index % defaultColors.length];
    return {
      id: `opt_${Date.now()}_${index}_${Math.random().toString(36).substring(2, 6)}`,
      text: (text || '').trim(),
      color,
      count: 0
    };
  });

  if (formattedOptions.some(o => !o.text)) {
    return res.status(400).json({ error: 'All option texts must be non-empty' });
  }

  const newPoll: Poll = {
    id: `poll_${Date.now().toString(36)}_${Math.random().toString(36).substring(2, 7)}`,
    creatorId: user.id,
    creatorName: user.name,
    question: question.trim(),
    description: description ? description.trim() : '',
    category: category ? category.trim() : 'General',
    options: formattedOptions,
    isClosed: false,
    allowMultiple: !!allowMultiple,
    totalVotes: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    expiresAt: expiresAt || null
  };

  db.polls.push(newPoll);
  db.save();

  // Register in Redis atomic counter
  redis.registerPoll(newPoll);

  res.status(201).json({ poll: newPoll });
});

app.patch('/api/polls/:id/status', authenticateToken, (req, res) => {
  const userId = (req as any).user.id;
  const pollId = req.params.id;
  const { isClosed } = req.body;

  const poll = db.polls.find(p => p.id === pollId);
  if (!poll) {
    return res.status(404).json({ error: 'Poll not found' });
  }

  if (poll.creatorId !== userId) {
    return res.status(403).json({ error: 'You are not authorized to modify this poll' });
  }

  poll.isClosed = !!isClosed;
  poll.updatedAt = new Date().toISOString();
  db.save();

  // Broadcast poll status update
  redis.publish(`poll:${poll.id}`, {
    type: 'poll_status_changed',
    pollId: poll.id,
    isClosed: poll.isClosed
  });

  res.json({ poll });
});

app.delete('/api/polls/:id', authenticateToken, (req, res) => {
  const userId = (req as any).user.id;
  const pollId = req.params.id;

  const pollIndex = db.polls.findIndex(p => p.id === pollId);
  if (pollIndex === -1) {
    return res.status(404).json({ error: 'Poll not found' });
  }

  const poll = db.polls[pollIndex];
  if (poll.creatorId !== userId) {
    return res.status(403).json({ error: 'Only the creator can delete this poll' });
  }

  db.polls.splice(pollIndex, 1);
  // Remove related votes
  const remainingVotes = db.votes.filter(v => v.pollId !== pollId);
  db.votes.length = 0;
  db.votes.push(...remainingVotes);
  db.save();

  // Clean Redis memory
  redis.removePoll(pollId);

  // Broadcast poll deletion
  redis.publish(`poll:${pollId}`, {
    type: 'poll_deleted',
    pollId
  });

  res.json({ success: true, message: 'Poll deleted successfully' });
});

// -------------------------------------------------------------
// Public Voting & Real-Time APIs (Open with 1-Person-1-Vote Rules)
// -------------------------------------------------------------
app.get('/api/polls/:id', optionalAuthenticate, (req, res) => {
  const pollId = req.params.id;
  const user = (req as any).user;
  const poll = db.polls.find(p => p.id === pollId);

  if (!poll) {
    return res.status(404).json({ error: 'Poll not found' });
  }

  // Get live counts from Redis atomic memory
  const liveCounts = redis.getCounts(poll.id);
  let totalVotes = 0;
  const optionsWithPercentages = poll.options.map(opt => {
    const count = liveCounts[opt.id] !== undefined ? liveCounts[opt.id] : opt.count;
    totalVotes += count;
    return { ...opt, count };
  });

  // Calculate percentages
  const enrichedOptions = optionsWithPercentages.map(opt => ({
    ...opt,
    percentage: totalVotes > 0 ? Math.round((opt.count / totalVotes) * 1000) / 10 : 0
  }));

  // Check if current visitor has already voted
  const voterKey = calculateVoterKey(req, pollId, user?.id);
  const existingVote = db.votes.find(v => v.pollId === pollId && v.voterKey === voterKey);

  res.json({
    poll: {
      ...poll,
      options: enrichedOptions,
      totalVotes
    },
    hasVoted: !!existingVote,
    userVote: existingVote ? existingVote.selectedOptionIds : null
  });
});

app.post('/api/polls/:id/vote', optionalAuthenticate, (req, res) => {
  const pollId = req.params.id;
  const user = (req as any).user;
  const { optionIds } = req.body;

  const poll = db.polls.find(p => p.id === pollId);
  if (!poll) {
    return res.status(404).json({ error: 'Poll not found' });
  }

  if (poll.isClosed) {
    return res.status(400).json({ error: 'This poll has ended and is no longer accepting votes' });
  }

  // Check expiration if set
  if (poll.expiresAt && new Date(poll.expiresAt) < new Date()) {
    poll.isClosed = true;
    db.save();
    return res.status(400).json({ error: 'This poll has expired' });
  }

  if (!Array.isArray(optionIds) || optionIds.length === 0) {
    return res.status(400).json({ error: 'Please select at least one option to vote' });
  }

  if (!poll.allowMultiple && optionIds.length > 1) {
    return res.status(400).json({ error: 'This poll only allows a single selection' });
  }

  // Validate that all optionIds belong to this poll
  const validOptionIds = new Set(poll.options.map(o => o.id));
  for (const optId of optionIds) {
    if (!validOptionIds.has(optId)) {
      return res.status(400).json({ error: 'Invalid option selected' });
    }
  }

  // Enforce strict 1-person-1-vote
  const voterKey = calculateVoterKey(req, pollId, user?.id);
  const existingVote = db.votes.find(v => v.pollId === pollId && v.voterKey === voterKey);
  if (existingVote) {
    return res.status(409).json({
      error: 'One person, one vote policy enforced. You have already cast your ballot in this poll.',
      hasVoted: true,
      userVote: existingVote.selectedOptionIds
    });
  }

  // 1. Atomic Redis INCR counter operation
  const updatedCounts = redis.atomicIncr(pollId, optionIds);

  // 2. Persist vote record to MongoDB document store
  const forwarded = req.headers['x-forwarded-for'];
  const ip = typeof forwarded === 'string' ? forwarded.split(',')[0].trim() : req.socket.remoteAddress || 'unknown';

  const newVote: VoteRecord = {
    id: `vote_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
    pollId,
    voterKey,
    selectedOptionIds: optionIds,
    ip,
    createdAt: new Date().toISOString()
  };

  db.votes.push(newVote);

  // Sync back to poll document
  let totalVotes = 0;
  for (const opt of poll.options) {
    if (updatedCounts[opt.id] !== undefined) {
      opt.count = updatedCounts[opt.id];
    }
    totalVotes += opt.count;
  }
  poll.totalVotes = totalVotes;
  poll.updatedAt = new Date().toISOString();
  db.save();

  // Compute live percentages
  const enrichedOptions = poll.options.map(opt => ({
    ...opt,
    count: updatedCounts[opt.id] || 0,
    percentage: totalVotes > 0 ? Math.round(((updatedCounts[opt.id] || 0) / totalVotes) * 1000) / 10 : 0
  }));

  // 3. Publish atomic event to Redis Pub/Sub
  const pubSubPayload = {
    type: 'vote_cast',
    pollId,
    totalVotes,
    options: enrichedOptions,
    timestamp: new Date().toISOString()
  };

  redis.publish(`poll:${pollId}`, pubSubPayload);

  res.status(200).json({
    success: true,
    message: 'Your vote has been counted!',
    poll: {
      ...poll,
      options: enrichedOptions,
      totalVotes
    },
    userVote: optionIds
  });
});

// List public trending polls
app.get('/api/polls', (req, res) => {
  const publicPolls = db.polls
    .filter(p => !p.isClosed)
    .sort((a, b) => b.totalVotes - a.totalVotes)
    .slice(0, 12);

  const enriched = publicPolls.map(poll => {
    const liveCounts = redis.getCounts(poll.id);
    let total = 0;
    const opts = poll.options.map(o => {
      const count = liveCounts[o.id] !== undefined ? liveCounts[o.id] : o.count;
      total += count;
      return { ...o, count };
    });
    return {
      ...poll,
      options: opts,
      totalVotes: total
    };
  });

  res.json({ polls: enriched });
});

// -------------------------------------------------------------
// HTTP & WebSocket Server Setup
// -------------------------------------------------------------
async function startServer() {
  const server = createHttpServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });

  // Map to track client subscriptions: WebSocket -> Set<pollId>
  const clientSubscriptions = new Map<WebSocket, Set<string>>();

  // Global redis subscription bridge for real-time WebSocket broadcasting
  for (const poll of db.polls) {
    redis.subscribe(`poll:${poll.id}`, (payload) => {
      const msg = JSON.stringify(payload);
      for (const [client, rooms] of clientSubscriptions.entries()) {
        if (client.readyState === WebSocket.OPEN && rooms.has(poll.id)) {
          client.send(msg);
        }
      }
    });
  }

  wss.on('connection', (ws: WebSocket, req) => {
    clientSubscriptions.set(ws, new Set());

    ws.on('message', (data: string) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'subscribe' && msg.pollId) {
          const rooms = clientSubscriptions.get(ws);
          if (rooms) {
            rooms.add(msg.pollId);
            // Send immediate snapshot of latest counts
            const liveCounts = redis.getCounts(msg.pollId);
            const poll = db.polls.find(p => p.id === msg.pollId);
            if (poll) {
              let total = 0;
              for (const opt of poll.options) {
                total += liveCounts[opt.id] !== undefined ? liveCounts[opt.id] : opt.count;
              }
              const options = poll.options.map(opt => {
                const count = liveCounts[opt.id] !== undefined ? liveCounts[opt.id] : opt.count;
                return {
                  ...opt,
                  count,
                  percentage: total > 0 ? Math.round((count / total) * 1000) / 10 : 0
                };
              });
              ws.send(JSON.stringify({
                type: 'snapshot',
                pollId: poll.id,
                totalVotes: total,
                options,
                isClosed: poll.isClosed
              }));
            }
          }
        } else if (msg.type === 'unsubscribe' && msg.pollId) {
          const rooms = clientSubscriptions.get(ws);
          if (rooms) {
            rooms.delete(msg.pollId);
          }
        } else if (msg.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong' }));
        }
      } catch (err) {
        console.error('Error handling WebSocket message:', err);
      }
    });

    ws.on('close', () => {
      clientSubscriptions.delete(ws);
    });

    ws.on('error', (err) => {
      console.error('WebSocket connection error:', err);
      clientSubscriptions.delete(ws);
    });
  });

  // Attach dynamic Redis channel subscription for newly created polls
  const originalRegisterPoll = redis.registerPoll.bind(redis);
  redis.registerPoll = function (newPoll: Poll) {
    originalRegisterPoll(newPoll);
    redis.subscribe(`poll:${newPoll.id}`, (payload) => {
      const msg = JSON.stringify(payload);
      for (const [client, rooms] of clientSubscriptions.entries()) {
        if (client.readyState === WebSocket.OPEN && rooms.has(newPoll.id)) {
          client.send(msg);
        }
      }
    });
  };

  // Setup Vite dev server or static distribution
  const isProduction = process.env.NODE_ENV === 'production';
  if (!isProduction) {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa'
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(path.resolve(process.cwd(), 'dist')));
    app.get('*', (req, res) => {
      res.sendFile(path.resolve(process.cwd(), 'dist', 'index.html'));
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`PulseVote SaaS Server running on http://0.0.0.0:${PORT}`);
  });
}

startServer().catch(err => {
  console.error('Fatal server startup error:', err);
  process.exit(1);
});
