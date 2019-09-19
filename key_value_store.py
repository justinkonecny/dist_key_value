#!/usr/bin/env python3

import sys, socket, select, time, json, random, math, enum, uuid, queue

APPEND_RESPONSE = "appendResponse"
DURABLE = "durable"
APPEND = "appendEntry"
REQUEST = "requestVote"
VOTE = "responseVote"
REDIRECT = "redirect"
FAIL = "fail"
PUT = "put"
GET = "get"
OK = "ok"

TIME_LOWER = 200.0  # the lower timeout limit
TIME_UPPER = 400.0  # the upper timeout limit

QUORUM_TIMEOUT = 0.25  # the timeout for quorum before retransmission (seconds)
DEAD_COUNT = 100  # the number of entires behind before a replica is considered dead
QUORUM_ATTEMPTS = 20
TOTAL_DROPS = 0
MSG_LEN_LIMIT = 30

DEBUG = False
DEBUG_QUOR = False
DEBUG_BUFF = False
DEBUG_TYPE = False

class State(enum.Enum):
	FOLL = "FOLLOWER"
	CAND = "CANDIDATE"
	LEAD = "LEADER"

class Server:
	def __init__(self):
		self.my_id = None  # server ID number
		self.replica_ids = None  # ID numbers of all the other replicas
		self.sock = None

		self.state = State.FOLL  # this server's current state (FOLL, CAND, LEAD)
		self.last_timeout = 0.0  # the time the last heartbeat was received
		self.last_hb_sent = 0.0  # the time the leader last sent a heartbeat
		self.clock = 0.0
		self.majority = 0  # the majority count (number needed for election)
		self.term = 0  # the current term
		self.leader = 'FFFF'  # id of the the server believed to be leader
		self.voted_for = []  # terms that this server has voted in

		self.log = []  # this server's log
		self.data = {}  # this server's data
		self.index_temp = 0  # index of highest log entry known to be committed
		self.index_durable = 0  # index of highest log entry applied
		self.buffer_soc = queue.Queue()  # buffer of incoming messages
		self.buffer_put = queue.Queue()  # buffer of outgoing messages
		self.buffer_put_limit = 1

		self.in_partition = False

		self.randomize_timer()
		self.setup_connection()

	def randomize_timer(self):
		''' Sets the election timer to a random value and adjusts hearbeat timeout '''
		self.election_timeout = (random.randint(TIME_LOWER, TIME_UPPER) / 100.0)  #TODO ADJUST TO MS
		self.heartbeat_timeout = (self.election_timeout / 6.0)  #TODO ADJUST SECONDS
		return

	def setup_connection(self):
		''' Sets up this server's connections with all other replicas '''
		self.my_id = sys.argv[1]
		self.replica_ids = sys.argv[2:]
		self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
		self.sock.connect(self.my_id)  # connect to the network
		self.majority = math.floor(len(self.replica_ids) / 2) + 1

		self.still_alive = self.replica_ids.copy()
		self.rep_log_index = {}
		for id in self.replica_ids:
			self.rep_log_index[id] = 0
		return

	def run(self):
		''' Begins this server's loop of listening and responding over sockets '''
		while True:
			if self.in_partition:
				continue

			# leader timeout for sending heartbeats
			if (self.state == State.LEAD) and (self.clock - self.last_hb_sent > self.heartbeat_timeout):
				self.send_heartbeat()

			msg = None
			if self.buffer_put.qsize() >= self.buffer_put_limit:
				# send appendEntry messages for all buffered puts
				self.dispatch_put_buffer()
				msg = None
			elif not self.leader == 'FFFF' and not self.buffer_soc.empty():
				# read from the buffered client/replica messages
				msg = self.buffer_soc.get()
				print('read from buffer') if DEBUG_BUFF else None
			else:
				# read from the socket directly
				ready = select.select([self.sock], [], [], 0.1)[0]
				if self.sock in ready:
					print('{} buff_soc {} :: buff_put {}'.format(self.my_id, self.buffer_soc.qsize(), self.buffer_put.qsize())) if DEBUG_BUFF else None
					msg_raw = self.sock.recv(32768)
					if len(msg_raw) == 0: continue
					msg = json.loads(msg_raw.decode())

			if not msg == None:
				if msg['type'] == APPEND:
					self.append_entry(msg)
				elif msg['type'] == REQUEST:
					self.request_vote(msg)
				elif msg['type'] == VOTE:
					# candidate drops to follower, then receives vote responses
					pass
				elif msg['type'] == APPEND_RESPONSE:
					self.process_append_response(msg)
				elif self.leader == 'FFFF':
					print('add to buffer main') if DEBUG_BUFF else None
					self.buffer_soc.put(msg)
				elif msg['type'] == GET:
					self.get(msg)
				elif msg['type'] == PUT:
					self.put(msg)
				else:
					print('unknown message type:', msg['type']) if DEBUG else None

			self.clock = time.time()
			# follower timeout for election (indicates last heard heartbeat)
			if (not self.state == State.LEAD) and (self.clock - self.last_timeout > self.election_timeout):
				self.begin_election()
		return

	# State: ANY
	def get(self, msg):
		''' Responds with the appropriate value for the given key '''
		if not self.state == State.LEAD:
			self.redirect(msg)
		else:
			print('get') if DEBUG_TYPE else None
			key = msg['key']
			if key in self.data:
				value = self.data[key]
				response_value = {
					'src': self.my_id,
					'dst': msg['src'],
					'leader': self.leader,
					'type': OK,
					'MID': msg['MID'],
					'value': value
				}
				self.send(response_value)
			else:
				print(self.my_id, 'failed to find key', key) if DEBUG else None
				# self.send_fail(msg)
		return

	# State: ANY
	def put(self, msg):
		if not self.state == State.LEAD:
			self.redirect(msg)
		else:
			print('put') if DEBUG_TYPE else None
			self.buffer_put.put(msg)
		return

	# State: Leader
	def dispatch_put_buffer(self):
		if self.buffer_put.empty():
			return

		print('dispatch puts from', self.my_id) if DEBUG else None
		entry_list = []
		msg_list = []
		while not self.buffer_put.empty():
			msg = self.buffer_put.get()
			msg_list.append(msg)
			key = msg['key']
			value = msg['value']

			log_entry = {
				'term': self.term,
				'key': key,
				'value': value,
				'MID': msg['MID'],
				'client': msg['src']
			}
			self.log.append(log_entry)
			entry_list.append(log_entry)

		success = self.process_command(entry_list)
		if not success:
			return False

		for msg in msg_list:
			response_ok = {
				'src': self.my_id,
				'dst': msg['src'],
				'leader': self.leader,
				'type': OK,
				'MID': msg['MID']
			}
			self.send(response_ok)
		return True

	def process_command(self, entry_list):
		# send append messages to all replicas
		mark_dead = []
		append_msgs = []
		#for id in self.still_alive:
		for id in self.replica_ids:
			log_list = []
			if self.rep_log_index[id] < len(self.log):
				if (len(self.log) - self.rep_log_index[id]) > DEAD_COUNT:
					if self.rep_log_index[id] > 5:
						mark_dead.append(id)  #TODO: REMOVE?

				for j in range(self.rep_log_index[id], len(self.log)):
					if (len(self.log) - self.rep_log_index[id]) > MSG_LEN_LIMIT and j > 50:
						print(self.my_id, 'LARGE CATCHUP', id, len(self.log) - self.rep_log_index[id]) if DEBUG else None
						break

					new_msg = self.log[j]
					key = new_msg['key']
					value = new_msg['value']
					log_entry = {
						'term': self.term,
						'key': key,
						'value': value,
						'MID': new_msg['MID'],
						'client': new_msg['client']
					}
					log_list.append(new_msg)

			append_msg = {
				'src': self.my_id,
				'dst': id,
				'leader': self.leader,
				'term': self.term,
				'type': APPEND,
				'start': self.rep_log_index[id],
				'prevLogIndex': self.index_durable,
				'prevLogTerm': self.log[self.index_durable]['term'],
				'entry': log_list,
				'leaderCommit': self.index_durable,
				'logLength': len(self.log)
			}
			append_msgs.append(append_msg)

		self.last_hb_sent = time.time()

		for a_msg in append_msgs:
			self.send(a_msg)


		#TODO: REMOVE?
		for serv in mark_dead:
			if serv in self.still_alive:
				print('MARKING', serv, 'AS DEAD') if DEBUG else None
				self.still_alive.remove(serv)

		print('waiting') if DEBUG_QUOR else None
		global TOTAL_DROPS
		# wait for quorum responses
		quorum = { self.my_id }
		time_start = time.time()
		attempt = 0
		while len(quorum) < self.majority:
			if time.time() - time_start > QUORUM_TIMEOUT:
				attempt += 1
				if attempt >= QUORUM_ATTEMPTS:
					print('PROBABLY PARTITION', self.my_id) if DEBUG else None
					self.state = State.FOLL
					self.leader = 'FFFF'
					return False

				print('RESEND REQUEST', TOTAL_DROPS) if DEBUG else None
				TOTAL_DROPS += 1

				for a_msg in append_msgs:
					self.send(a_msg)

			ready = select.select([self.sock], [], [], 0.1)[0]
			if self.sock in ready:
				msg_raw = self.sock.recv(32768)
				if len(msg_raw) == 0: continue
				msg_resp = json.loads(msg_raw.decode())

				if msg_resp['type'] == APPEND_RESPONSE and (msg_resp['src'] not in quorum):
					src = msg_resp['src']
					quorum.add(src)
					old_index = self.rep_log_index[src]
					new_index = msg_resp['logLength']

					if new_index > old_index:
						self.rep_log_index[src] = new_index

				elif msg_resp['type'] == GET:
					self.get(msg_resp)
				else:
					self.buffer_soc.put(msg_resp)
					print('add to buffer quorum: {}/{}'.format(quorum, self.majority)) if DEBUG_QUOR else None
		print('quorum reached') if DEBUG_QUOR else None

		# make change durable
		self.index_durable += len(entry_list)
		for msg in entry_list:
			self.data[msg['key']] = msg['value']

		print(self.my_id, 'len:', len(self.log), 'durable:', self.index_durable, self.log[-1]['key'], self.log[-1]['value']) if DEBUG else None
		return True

	# State: Leader
	def finalize_log(self):
		if len(self.log) > self.index_durable:
			for i in range(self.index_durable, len(self.log)):
				last = self.log[self.index_durable]
				key = last['key']
				value = last['value']
				client = last['client']
				mid = last['MID']

				self.data[key] = value
				self.index_durable += 1

				response_ok = {
					'src': self.my_id,
					'dst': client,
					'leader': self.leader,
					'type': OK,
					'MID': mid
				}
				self.send(response_ok)
		return

	# State: Any
	def process_append_response(self, msg):
		if self.state == State.LEAD:
			src = msg['src']
			old_index = self.rep_log_index[src]
			new_index = msg['logLength']
			if new_index > old_index:
				self.rep_log_index[src] = new_index
		else:
			self.last_timeout = time.time()
		return

	# State: Follower
	def append_entry(self, msg):
		print('append') if DEBUG_TYPE else None
		self.last_timeout = time.time()

		# reset timer on heart beat
		if msg['leader'] != self.leader:
			if self.leader != 'FFFF' and self.leader in self.still_alive:
				self.still_alive.remove(self.leader)

			if msg['term'] > self.term:
				self.term = msg['term']

			self.leader = msg['leader']
			new_leader_response = {
				'src': self.my_id,
				'dst': msg['src'],
				'leader': self.leader,
				'type': APPEND_RESPONSE,
				'logLength': len(self.log)
			}
			self.send(new_leader_response)

		if 'entry' in msg:
			if not msg['start'] <= len(self.log):
				print(self.my_id, "SOMETHING WRONG logLen", len(self.log), "start", msg['start']) if DEBUG else None
				return

			index = msg['start']
			for msg_entry in msg['entry']:
				if index < len(self.log):
					#TODO MAYBE REMOVE?
					self.log[index] = msg_entry
					# pass
				else:
					print(self.my_id, 'adding', msg_entry['key'], msg_entry['value']) if DEBUG else None
					self.log.append(msg_entry)
				index += 1

			append_response = {
				'src': self.my_id,
				'dst': msg['src'],
				'leader': self.leader,
				'type': APPEND_RESPONSE,
				'logLength': len(self.log)
			}
			# print(self.my_id, "loglen", len(self.log))
			self.send(append_response)
		# else:
		leaderCommit = msg['leaderCommit']
		if leaderCommit > self.index_durable:
			for i in range(self.index_durable, min(leaderCommit, len(self.log))):
				entry = self.log[i]
				key = entry['key']
				value = entry['value']
				self.data[key] = value
			self.index_durable = min(leaderCommit, len(self.log))
		self.last_timeout = time.time()
		return

	# State: Leader
	def send_fail(self, msg):
		print(self.my_id, 'SENDING FAIL') if DEBUG else None
		response_fail = {
			'src': self.my_id,
			'dst': msg['src'],
			'leader': self.leader,
			'type': FAIL,
			'MID': msg['MID']
		}
		self.send(response_fail)
		return

	# State: Follower
	def redirect(self, msg):
		print('redirect from {} to {}'.format(self.my_id, self.leader)) if DEBUG else None
		response = {
			'src': self.my_id,
			'dst': msg['src'],
			'leader': self.leader,
			'type': REDIRECT,
			'MID': msg['MID']
		}
		self.send(response)

		#TODO REMOVE
		self.last_timeout -= (self.election_timeout / 7.0)

		return

	# State: Follower OR Candidate
	def request_vote(self, msg):
		vote_granted = False

		if not msg['src'] in self.still_alive:
			self.still_alive.append(msg['src'])
			print(self.my_id, 'MARKING', msg['src'], 'ALIVE') if DEBUG else None

		if self.state == State.FOLL or self.state == State.CAND:
			first_vote = (msg['term'] not in self.voted_for)
			new_term = (msg['term'] >= self.term)
			updated_log = msg['logLength'] >= len(self.log)

			vote_granted = first_vote and new_term and updated_log  #TODO: CHECK CORRECTNESS, ADD LOG ASSERT

		vote_response = {
			'src': self.my_id,
			'dst': msg['src'],
			'leader': self.leader,
			'type': VOTE,
			'term': msg['term'],
			'vote': vote_granted,
			'logLength': len(self.log)
		}

		if vote_granted:
			print(self.my_id, 'vote request from', msg['src'], 'granted?', vote_granted, 'len:', len(self.log), 'durable:', self.index_durable, self.log[-1]['key'], self.log[-1]['value']) if (DEBUG and len(self.log) > 0) else None
		elif self.state == State.FOLL:
			print(self.my_id, 'vote request from', msg['src'], '| granted?', vote_granted, '| f?', first_vote, '| nt?', new_term, '| ul?', updated_log, 'len:', len(self.log), 'durable:', self.index_durable, self.log[-1]['key'], self.log[-1]['value']) if (DEBUG and len(self.log) > 0) else None
			print(self.my_id, 'myTerm', self.term, 'reqTerm', msg['term']) if DEBUG else None

		if vote_granted:
			self.voted_for.append(msg['term'])
		self.send(vote_response)
		return

	# State: Leader
	def send_heartbeat(self):
		print('sending heartbeat from', self.my_id) if DEBUG else None
		heartbeat = {
			'src': self.my_id,
			'dst': 'FFFF',
			'leader': self.leader,
			'type': APPEND,
			'term': self.term,
			'prevLogIndex': self.index_durable,
			# 'prevLogTerm': self.log[len(self.log) - 1]['term'],
			'leaderCommit': self.index_durable,
			'logLength': len(self.log)
		}
		self.last_hb_sent = self.clock
		self.send(heartbeat)

		print('{} buff_soc {} :: buff_put {}'.format(self.my_id, self.buffer_soc.qsize(), self.buffer_put.qsize())) if DEBUG_BUFF else None

		self.dispatch_put_buffer()
		return

	# State: Candidate
	def begin_election(self):
		print('begin election, candidate', self.my_id, 'logLen', len(self.log)) if DEBUG else None
		self.state = State.CAND
		self.last_timeout = self.clock
		self.randomize_timer()
		self.term += 1

		vote_request = {
			'src': self.my_id,
			'dst': 'FFFF',  # broadcast
			'leader': self.leader,
			'type': REQUEST,
			'term': self.term,
			'logLength': len(self.log)
		}

		self.send(vote_request)

		if TOTAL_DROPS >= 5:
			self.send(vote_request)

		self.election_loop()
		return

	# State: Candidate
	def election_loop(self):
		# waits for vote response and handles sending vote responses
		vote_count = 1
		time_start = time.time()
		attempt = 0
		while True:
			ready = select.select([self.sock], [], [], 0.1)[0]
			if self.sock in ready:
				msg_raw = self.sock.recv(32768)
				if len(msg_raw) == 0: continue
				msg = json.loads(msg_raw.decode())

				# handle a vote in favor of candidate
				if msg['type'] == VOTE:
					if msg['leader'] != 'FFFF' and msg['leader'] != self.leader:
						print('drop to follower', self.my_id) if DEBUG else None
						self.state = State.FOLL
						return

					elif msg['vote'] == True and msg['term'] == self.term:
						# count the vote
						vote_count += 1
						src = msg['src']
						old_index = self.rep_log_index[src]
						new_index = msg['logLength']
						if new_index > old_index:
							self.rep_log_index[src] = new_index

						if vote_count >= self.majority:
							# new leader
							print('leader elected as', self.my_id) if DEBUG else None

							if self.leader != 'FFFF':
								print('MARK OLD LEADER', self.leader, 'DEAD') if DEBUG else None
								if self.leader in self.still_alive:
									self.still_alive.remove(self.leader)
								print('NEW LEADER INDEX', self.index_durable) if DEBUG else None
								self.finalize_log()

							self.state = State.LEAD
							self.leader = self.my_id
							for _ in range(0, 5):
								self.send_heartbeat()
							return

				# handle a vote request (reject request with no vote)
				elif msg['type'] == REQUEST:
					# repond to vote request
					if msg['logLength'] > len(self.log):
						print('drop to follower', self.my_id) if DEBUG else None
						self.state = State.FOLL
						self.request_vote(msg)
						return
					elif msg['term'] > self.term and msg['logLength'] >= len(self.log):
						print('drop to follower', self.my_id) if DEBUG else None
						self.state = State.FOLL
						self.request_vote(msg)
						return
					else:
						self.request_vote(msg)

				# handle heartbeat from new leader
				elif msg['type'] == APPEND: #and msg['term'] >= self.term:  #TODO CHECK LEADER TERM
					if msg['logLength'] >= len(self.log): #TODO
						print('drop to follower', self.my_id) if DEBUG else None
						#TODO ACCEPT LEADER AFTER CHECKING COMMIT INDEX
						self.state = State.FOLL
						self.append_entry(msg)
						return

				else:
					self.buffer_soc.put(msg)
					print('add to buffer election') if DEBUG_BUFF else None


			# election timeout, restart election
			self.clock = time.time()
			if self.clock - self.last_timeout > (self.election_timeout * 0.50):
				self.begin_election()
				return
		return

	# State: ANY
	def send(self, msg):
		self.sock.send(json.dumps(msg).encode())

def main():
	server = Server()
	server.run()
	return

if __name__ == "__main__":
	main()
