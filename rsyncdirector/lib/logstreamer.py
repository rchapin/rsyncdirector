# import io
# from rsyncdirector.lib.logging import Logger


# class LogStreamer(io.TextIOBase):
#     """A file-like object that redirects writes to a logger."""

#     def __init__(self, logger: Logger, component: str):
#         self.logger = logger.bind(component=component)
#         self.buffer = ""

#     def write(self, message: str) -> int:
#         # Sub-processes often stream in chunks, not full lines so we buffer until we see a newline.
#         self.buffer += message
#         if "\n" in self.buffer:
#             lines = self.buffer.split("\n")

#             # Log all complete lines
#             for line in lines[:-1]:
#                 clean_line = line.strip()
#                 if clean_line:
#                     # This triggers the JSON output with all of the provided metadata.
#                     # self.logger.info("rsync_output", subprocess_msg=clean_line)
#                     self.logger.info(clean_line)

#             # Keep the partial line for the next write
#             self.buffer = lines[-1]
#         return len(message)

#     def flush(self):
#         if self.buffer.strip():
#             # self.logger.info("rsync_output", subprocess_msg=self.buffer.strip())
#             self.logger.info(self.buffer.strip())
#             self.buffer = ""
