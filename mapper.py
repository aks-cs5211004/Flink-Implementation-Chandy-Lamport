import logging
import os
import queue
import signal
import socket
import threading
import time
import pandas as pd
from _socket import SHUT_RDWR
from abc import abstractmethod, ABC
from collections import Counter
from dataclasses import dataclass, field
from multiprocessing import Process
from typing import Final, List


import redis

from constants import STREAMS, COORDINATOR_PORT, HEARTBEAT_INTERVAL, FNAME
from message import Message, MT
from mylog import Logger

logging = Logger().get_logger()


@dataclass
class MapperState:
  idx: int
  reducer_ports: list[int]
  pid: int
  id: str
  last_cp_id: int
  c_socket: socket.socket
  last_stream_id: bytes = b"0"
  last_recovery_id: int = 0
  reducer_sockets: list[socket.socket] = field(default_factory=list)
  is_wc_done: bool = False

  # TCP Connection to the reducers
  def __post_init__(self):
    self.c_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logging.info(f"{self.id} Connecting to reducers")
    for i in range(len(self.reducer_ports)):
      conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      while True:
        try:
          conn.connect(("localhost", self.reducer_ports[i]))
          break
        except ConnectionRefusedError:
          # Keep trying, eventually reducer will be up.
          logging.warning("Couldn't connect to reducer, waiting for it go up.")
          time.sleep(0.2)

      self.reducer_sockets.append(conn)
    
  # use this function to send any message to the coordinator
  def to_coordinator(self, msg: Message):
    self.c_socket.sendto(msg.serialize(), ("localhost", COORDINATOR_PORT))

  # use this function to create a mapper's checkpoint
  def checkpoint(self, checkpoint_id: int):
    os.makedirs('checkpoints', exist_ok=True)
    filename = f"checkpoints/{self.id}_{checkpoint_id}.txt"
    # logging.info(f"{self.id} checkpoint at {self.last_stream_id.decode()}")
    with open(filename, 'w') as file:
      file.write(self.last_stream_id.decode() + '\n')
      
  # TCP Connection to the reducers
  def post_init_help(self):
    for i, conn in enumerate(self.reducer_sockets):
        try:
            # Check if the socket is already connected
            if conn.fileno() != -1:
                conn.getpeername()  # Check if the socket is connected
                # logging.info(f"Already connected to reducer at port {self.reducer_ports[i]}")
                continue
        except OSError:
            # If the socket is invalid or not connected, close and recreate it
            # logging.info(f"Reconnecting to reducer at port {self.reducer_ports[i]}")
            conn.close()  # Close the old socket
            self.reducer_sockets[i] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Recreate the socket
            conn = self.reducer_sockets[i]  # Update the socket reference

        # Try connecting to the reducer again
        while True:
            try:
                result = conn.connect_ex(("localhost", self.reducer_ports[i]))  # Attempt to connect
                if result == 0:  # Success
                    # logging.info(f"Connected to reducer at port {self.reducer_ports[i]}")
                    break
                else:
                    # logging.warning(f"Retrying connection to reducer at port {self.reducer_ports[i]}")
                    time.sleep(0.1)  # Wait before retrying
            except OSError as e:
                # logging.error(f"Error connecting to reducer at port {self.reducer_ports[i]}: {e}")
                time.sleep(0.1)  # Retry after delay
    # logging.info(f"{self.id} Successfully connected to all reducers")



  # mapper recovering
  def recover(self, recovery_id: int, checkpoint_id: int):
    # self.last_recovery_id = recovery_id
    # TODO
    checkpoint_file = f"checkpoints/{self.id}_{checkpoint_id}.txt"
    if os.path.exists(checkpoint_file):
      with open(checkpoint_file, 'r') as file:
        last_stream_id = file.readline().strip()
        self.last_stream_id = last_stream_id.encode() 
        # logging.info(f"{self.id} recover at {self.last_stream_id.decode()}")
    else:
      logging.error(f"Checkpoint file {checkpoint_file} not found for recovery.")
      self.last_stream_id = b"0"
    self.last_recovery_id = recovery_id
    self.last_cp_id=checkpoint_id
    self.is_wc_done=False
    
  # this hashing function will tell, to which reducer we should send a corresponding word
  # it returns the socket of the reducer
  def word_2_socket(self, word: str) -> socket.socket:
      if word[0] < 'm': # hashing function
        return self.reducer_sockets[0]
      else:
        return self.reducer_sockets[1]

  # function to exit the worker
  def exit(self):
    for rs in self.reducer_sockets:
      try:
        rs.shutdown(SHUT_RDWR)
      except Exception as e:
        logging.warning(f"Ignoring {e}")
    self.reducer_sockets = []

    os.kill(self.pid, signal.SIGKILL)


@dataclass
class Cmd(ABC):
  @abstractmethod
  def handle(self, state: MapperState):
    """
      Do not implement this, this is an abstract method. Implement this in child classes.
      Input: state -> Current state of the mapper, 
      Output: None

      Semantics of the function is as follows:- Upon receiving some command, take appropriate 
      actions and send messages to others(if required).
    """
    pass


@dataclass
class Checkpoint(Cmd):
  checkpoint_id: int
  recovery_id: int

  def handle(self, state: MapperState):
    # TODO: this mapper received checkpoint marker from coordinator. Take appropriate actions.
    # logging.info(f"{state.id} received Checkpoint {self.checkpoint_id}, Now Checkpointing Mapper")
    state.checkpoint(self.checkpoint_id)
    state.post_init_help()
    for r_sock in state.reducer_sockets:
        try:
          cp_message = Message(msg_type=MT.FWD_CHECKPOINT,source=state.id, mapper_id= state.id,checkpoint_id=self.checkpoint_id,last_recovery_id=self.recovery_id)
          try:
              r_sock.sendall(cp_message.serialize())
              # logging.info(f"{state.id} sent checkpoint marker with chekpoint id {self.checkpoint_id}  and recovery id {self.recovery_id}")
          except (BrokenPipeError, ConnectionError, TimeoutError) as e:
              logging.error(f"Failed to send checkpoint marker {self.checkpoint_id} to reducer: {e}")
              time.sleep(0.1)
        except (OSError, BrokenPipeError, ConnectionError, TimeoutError) as e:
            logging.error(f"Failed to connect or send checkpoint marker {self.checkpoint_id} to reducer: {e}")
            
    state.last_cp_id = self.checkpoint_id
    if self.checkpoint_id == 0:
        state.to_coordinator(Message(msg_type=MT.LAST_CHECKPOINT_ACK, source=state.id,checkpoint_id=self.checkpoint_id))
    else:
        state.to_coordinator(Message(msg_type=MT.CHECKPOINT_ACK, source=state.id,checkpoint_id=self.checkpoint_id))
    # logging.info(f"{state.id} Sent CP Ack for checkpoint {self.checkpoint_id} with recovery_id {self.recovery_id}")
    return

@dataclass
class Recover(Cmd):
  checkpoint_id: int
  recovery_id: int

  def handle(self, state: MapperState):
    # TODO: This mapper need to recover from failure.
    # logging.info(f"{state.id} Recovering to consistent checkpoint {self.checkpoint_id} with recovery_id {self.recovery_id}")
    state.recover(self.recovery_id, self.checkpoint_id)
    # logging.info(f"{state.id} Sending Recover Ack for consistent checkpoint {self.checkpoint_id} with recovery_id {self.recovery_id}")
    state.to_coordinator(Message(msg_type=MT.RECOVERY_ACK, source=state.id,
                                 recovery_id=self.recovery_id))
    done_msg = Message(msg_type=MT.DONE, source=state.id, msg= 'not_done')
    state.to_coordinator(done_msg)


@dataclass
class Exit(Cmd):
  # This is how EXIT request will be handled.
  def handle(self, state: MapperState):
    logging.critical(f"{state.id} exiting!")
    state.exit()


class CmdHandler(threading.Thread):
  def __init__(self, state: MapperState, cmd_q: queue.Queue[Cmd], stream_name: bytes):
    super(CmdHandler, self).__init__()
    self.cmd_q = cmd_q
    self.state = state
    self.rds = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    self.my_stream: bytes = stream_name
  
  # in every iteration, this thread will read the queue to process any request
  # else will do word count, if the queue is empty
  def run(self) -> None:
    while True:
      try:
        cmd: Cmd = self.cmd_q.get(block=self.state.is_wc_done) # when the word count is finished, block here.
      except queue.Empty:
        # if no request in queue, do word count.
        self.word_count()
        continue
      try:
        cmd.handle(self.state)
      except Exception as e:
        logging.exception(e)

  def word_count(self):
    result = self.rds.xread({self.my_stream: self.state.last_stream_id}, count = 1)
    if not result:
      self.state.is_wc_done = True
      # logging.info(f"{self.state.id} finished reading its stream!")
      logging.info(f"{self.state.id} telling the coordinator that I am done!")
      done_msg = Message(msg_type=MT.DONE, source=self.state.id, msg='done')
      self.state.to_coordinator(done_msg)
      return
    try:
      # logging.info(f"Read stream {self.my_stream} from {self.state.last_stream_id}")
      self.state.post_init_help()
      _, entries = result[0]
      message_id, message_data = entries[0]
      filepath = message_data[FNAME].decode('utf-8')
      df = pd.read_csv(filepath)
      all_text = ' '.join(df['text'])
      tokens = all_text.split(" ")
      word_counts = Counter(tokens)
      for word, count in word_counts.items():
        r_sock = self.state.word_2_socket(word)
        msg_bytes = Message(msg_type=MT.WORD_COUNT, source=self.state.id, key=word, value=count, last_recovery_id = self.state.last_recovery_id).serialize()
        r_sock.sendall(msg_bytes)
      self.state.last_stream_id = message_id
    except Exception as e:
      logging.error(e)
      time.sleep(0.1)
    

class Mapper(Process):
  def __init__(self, idx: int, downstream: List[int], port: int):
    super().__init__()
    self.idx = idx
    self.id: Final[str] = f"Mapper_{idx}"
    self.downstream = downstream # ports of reducers
    self.listenPort = port # will listen at this port
    self.my_stream = STREAMS[idx] # will read from this stream in Redis

  def send_heartbeat(self):
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    coordinator_addr = ("localhost", COORDINATOR_PORT)
    while True:
      try:
        pass
        heartbeat_socket.sendto(Message(msg_type=MT.HEARTBEAT, source=self.id).serialize(), coordinator_addr)
        time.sleep(HEARTBEAT_INTERVAL)
      except Exception as e:
        logging.error(f"Heartbeat error: {e}")

  def handle_coordinator(self, coordinator_conn: socket.socket, cmd_q: queue.Queue[Cmd]):
    while True:
      try:
        response, _ = coordinator_conn.recvfrom(1024)
        message: Message = Message.deserialize(response)
        # logging.info(f"{self.id} received message of type {message.msg_type.name} from {message.source}")
        # The work to be done after receiving this message is put in command queue, so that cmd thread
        # can process it and take actions. 
        if message.msg_type == MT.CHECKPOINT:
          recovery_id = message.kwargs['recovery_id']
          checkpoint_id = message.kwargs['checkpoint_id']
          # logging.info(f"{self.id} received checkpoint marker from {message.source} "
          #              f"with id = {checkpoint_id} and recovery_id = {recovery_id}")
          try:
            cmd_q.put(Checkpoint(checkpoint_id=checkpoint_id, recovery_id=recovery_id))
          except Exception as e:
            logging.error(f"Error: {e}")
        elif message.msg_type == MT.RECOVER:
          recovery_id = message.kwargs['recovery_id']
          checkpoint_id = message.kwargs['checkpoint_id']
          # logging.info(f"{self.id} received recover request from {message.source} "
                      #  f"with id = {checkpoint_id} and recovery_id = {recovery_id}")
          cmd_q.put(Recover(checkpoint_id=checkpoint_id, recovery_id=recovery_id))
        elif message.msg_type == MT.EXIT:
          # logging.info(f"{self.id} received EXIT marker from {message.source}")
          cmd_q.put(Exit())

      except Exception as e:
        logging.error(f"Error: {e}")
        
  def run(self):
    cmd_q = queue.Queue()

    # state of this mapper
    state = MapperState(
      reducer_ports=self.downstream,
      pid=self.pid,
      id=self.id,
      idx=self.idx,
      last_stream_id=b"0",
      last_recovery_id=0,
      last_cp_id=0,
      c_socket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    )

    # thread to handle messages from the coordinator
    state.c_socket.bind(("localhost", self.listenPort))
    coordinator_handler = threading.Thread(target=self.handle_coordinator, args=(state.c_socket, cmd_q,))
    coordinator_handler.start()

    # thread to send heartbeats to the coordinator
    heartbeat_thread = threading.Thread(target=self.send_heartbeat)
    heartbeat_thread.start()

    # thread to process the command queue
    cmd_thread = CmdHandler(state, cmd_q, self.my_stream)
    cmd_thread.start()

    # logging.info(f"{self.id} waiting for all threads to join")
    cmd_thread.join()
    coordinator_handler.join()
    heartbeat_thread.join()
