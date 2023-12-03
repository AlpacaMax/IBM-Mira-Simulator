import sys
import csv
import logging
from datetime import datetime
from enum import Enum


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler(f"{datetime.now().isoformat()}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


class Task:
    def __init__(self, job, duration):
        self.job = job
        self.nodes = []
        self.start_time = None
        self.end_time = None
        self.duration = duration

    def start(self, curr_time):
        self.start_time = curr_time

    def end(self, curr_time):
        self.end_time = curr_time


class JobStatus(Enum):
    CREATED = 0
    QUEUED = 1
    READY = 2
    RUNNING = 3
    ENDED = 4


class Job:
    def __init__(
        self,
        name,
        submitted_time,
        duration,
        num_of_requested_nodes,
        exit_code
    ):
        self.name = name
        self.num_of_requested_nodes = num_of_requested_nodes
        self.submitted_time = submitted_time
        self.start_time = None
        self.end_time = None
        self.duration = duration
        self.exit_code = exit_code
        self.status = JobStatus.CREATED
        self.midplanes = []

    def __str__(self):
        return f"{self.name} {self.submitted_time} {self.duration} {self.num_of_requested_nodes} {self.status}"

    def submit(self, curr_time):
        self.submitted_time = curr_time
        self.status = JobStatus.QUEUED
        logging.info(f"Job {self.name}: Submitted on Wallclock={self.submitted_time}")

    def start(self, curr_time):
        self.start_time = curr_time
        self.status = JobStatus.RUNNING
        self.end_time = self.start_time + self.duration
        logging.info(f"Job {self.name}: Start running on Wallclock={self.start_time}. Expect to stop on Wallclock={self.end_time}")

    def end(self, curr_time):
        self.end_time = curr_time
        self.status = JobStatus.ENDED
        logging.info(f"Job {self.name}: Stop on Wallclock={self.end_time}")

    def tick(self, curr_time):
        if self.status == JobStatus.READY:
            self.start(curr_time)

        if self.status == JobStatus.RUNNING and self.end_time <= curr_time:
            self.end(curr_time)

    def assign_midplanes(self, midplanes):
        self.midplanes = midplanes
        logging.info(f"Job {self.name}: Assigned onto Midplane={','.join([md.name for md in self.midplanes])}")

    def ready(self):
        self.status = JobStatus.READY
        logging.info(f"Job {self.name}: Ready to start")

    def is_ended(self):
        return self.status == JobStatus.ENDED


class Node:
    def __init__(self):
        self.job = None

    def __str__(self):
        return str(self.job)

    def assign_job(self, job):
        self.job = job

    def free_up(self):
        self.job = None


class MidPlaneStatus(Enum):
    VACANT = 0
    OCCUPIED = 1


class MidPlane:
    def __init__(self, name):
        self.name = name
        self.status = MidPlaneStatus.VACANT
        self.job = None
        self.nodes = [Node() for i in range(16*32)]

    def __str__(self):
        return f"{self.name}: {self.status} {self.job}"

    def assign_job(self, job):
        for n in self.nodes:
            n.assign_job(job)
        self.status = MidPlaneStatus.OCCUPIED
        self.job = job
        logging.info(f"Midplane {self.name}: Job={self.job.name} is assigned onto this midplane")

    def free_up(self):
        if self.is_vacant():
            logging.warning(f"Midplane {self.name}: Attempting to free up again")
        for n in self.nodes:
            n.free_up()
        self.status = MidPlaneStatus.VACANT
        logging.info(f"Midplane {self.name}: Frees up from Job={self.job.name}")
        self.job = None

    def is_vacant(self):
        return self.status == MidPlaneStatus.VACANT


class Bucket:
    def __init__(self):
        self.name = None
        self.access_history = []


class Queue:
    def __init__(self):
        self.data = []

    def __len__(self):
        return len(self.data)

    def is_empty(self):
        return len(self) == 0

    def enqueue(self, obj):
        self.data.append(obj)

    def dequeue(self):
        return self.data.pop(0)

    def peek(self):
        return self.data[0]


class Mira:
    def __init__(self):
        self.queue = Queue()
        self.running_jobs = []
        self.midplanes = []
        for i in range(96):
            self.midplanes.append(MidPlane(str(i)))
        self.simulated_wallclock = 0

    def __str__(self):
        result = ""
        result += f"Wallclock: {self.simulated_wallclock}\n"
        result += f"Number of jobs in queue: {len(self.queue)}\n"
        result += f"Number of running jobs: {len(self.running_jobs)}\n"
        result += "Midplanes: \n"
        for i in range(len(self.midplanes)):
            result += str(self.midplanes[i])

        return result

    def submit_job(self, new_job):
        self.queue.enqueue(new_job)
        new_job.submit(self.simulated_wallclock)

    def schedule(self):
        # Grab a job from a queue to run
        # Simple FCFS
        if not self.queue.is_empty():
            next_job = self.queue.peek()
            num_of_requested_nodes = next_job.num_of_requested_nodes
            num_of_requested_midplanes = num_of_requested_nodes // 512
            vacant_midplanes = []
            for mp in self.midplanes:
                if len(vacant_midplanes) == num_of_requested_midplanes:
                    break
                if mp.is_vacant():
                    vacant_midplanes.append(mp)

            if len(vacant_midplanes) == num_of_requested_midplanes:
                logging.info(f"Mira: Find vacant midplanes for Job={next_job.name}. Allocating job to midplanes and start executing")
                job = self.queue.dequeue()
                job.assign_midplanes(vacant_midplanes)
                for mp in vacant_midplanes:
                    mp.assign_job(job)
                job.ready()
                self.running_jobs.append(job)

    def jobs_tick(self, curr_wallclock):
        # Make every running job tick
        for j in self.running_jobs:
            j.tick(curr_wallclock)

            if j.is_ended():
                logging.info(f"Mira: Clean up Job={j.name}")
                for md in j.midplanes:
                    md.free_up()
                self.running_jobs.remove(j)

    def inc_wallclock(self):
        self.simulated_wallclock += 1


class Trace:
    def __init__(self):
        self.jobs = []

    def parse_trace_file(self, csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                job = Job(
                    name=row["JOB_NAME"],
                    submitted_time=int(datetime.fromisoformat(row["QUEUED_TIMESTAMP"]).timestamp()),
                    duration=int(datetime.fromisoformat(row["END_TIMESTAMP"]).timestamp())-int(datetime.fromisoformat(row["START_TIMESTAMP"]).timestamp()),
                    num_of_requested_nodes=int(float(row["NODES_REQUESTED"])),
                    exit_code=row["EXIT_STATUS"],
                )
                self.jobs.append(job)

        self.jobs.sort(reverse=True, key=lambda x:x.submitted_time)

        # Make the trace start with 0
        earliest = self.jobs[-1].submitted_time
        for j in self.jobs:
            j.submitted_time -= earliest

    def get_job(self, curr_time):
        if len(self.jobs) > 0 and self.jobs[-1].submitted_time == curr_time:
            return self.jobs.pop()


# Initialization
trace_filename = sys.argv[1]
trace = Trace()
mira = Mira()
trace.parse_trace_file(trace_filename)


# Simulation
while len(trace.jobs) > 0 or len(mira.queue) > 0 or len(mira.running_jobs) > 0:
    new_job = trace.get_job(mira.simulated_wallclock)
    while new_job is not None:
        print(len(trace.jobs))
        mira.submit_job(new_job)
        new_job = trace.get_job(mira.simulated_wallclock)

    mira.schedule()
    mira.jobs_tick(mira.simulated_wallclock)
    mira.inc_wallclock()
