class ClockIterClass:
    """Iterator class; adds 10 mins to the iterated variable"""
    def __init__(self, start="0000"):
       self.hh = start[0:2]; self.mm = start[2:]; self.time = start
    def __iter__(self):
       return self
    def __next__(self):
       if self.time == "2350":
          self.hh = "00"; self.mm = "00"; self.time = "0000"
          return self.time
       else: #for all other times
          if self.mm == "50":
             self.hh = hhmm_str( int(self.hh)+1 )
             self.mm = "00"
             self.time = self.hh + self.mm
             return self.time
          else: #self.hh remains unchanged!
             self.mm = str( int(self.mm)+10 )
             self.time = self.hh + self.mm
             return self.time
