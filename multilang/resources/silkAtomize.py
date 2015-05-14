import storm
import silk
import os

class silkBolt(storm.Bolt):
    callbacks = [
        "stime_epoch_secs", # Float
        "duration_secs", # Float
        "sport",     # Int
        "dport",     # Int
        "protocol",  # Int
        "classtype", # Tuple
        "sensor",    # String
        "tcpflags", # silk.TCPFlags (str)
        "initflags", # silk.TCPFlags (str)
        "restflags", # silk.TCPFlags (str)
        "application", # Int
        "input", # Int
        "output", # Int
        "packets", # Long
        "bytes", # Long
        "sip", # silk.IPv*Addr (str)
        "dip", # silk.IPv*Addr (str)
        "nhip", # silk.IPv*Addr (str)
        ]

    def process(self, tuple):
        # Assign the raw silk file path
        rawFile = tuple.values[0]

        # Open silk file
        sf = silk.SilkFile(rawFile, silk.READ)

        # Iterate over each record
        for rec in sf:
            # Necessary to process this way to ensure list order
            ret = [
                str(rec.stime_epoch_secs).split('.')[0], # Float
                str(rec.stime_epoch_secs).split('.')[1], # Float
                str(rec.duration_secs), # Float
                str(rec.sport),     # Int
                str(rec.dport),     # Int
                str(rec.protocol),  # Int
                str(rec.classtype[0]), # Tuple
                str(rec.classtype[1]), # Tuple
                str(rec.sensor),    # String
                str(rec.tcpflags), # silk.TCPFlags (str)
                str(rec.initflags), # silk.TCPFlags (str)
                str(rec.restflags), # silk.TCPFlags (str)
                str(rec.application), # Int
                str(rec.input), # Int
                str(rec.output), # Int
                str(rec.packets), # Long
                str(rec.bytes), # Long
                #str((rec.sip).to_ipv6()), # silk.IPv*Addr (str)
                #str((rec.dip).to_ipv6()), # silk.IPv*Addr (str)
                #str((rec.nhip).to_ipv6()), # silk.IPv*Addr (str)
                str(rec.sip), # silk.IPv*Addr (str)
                str(rec.dip), # silk.IPv*Addr (str)
                str(rec.nhip), # silk.IPv*Addr (str)
                ]

            storm.emit(ret)

        # Remove the raw silk file
        os.remove(rawFile)

silkBolt().run()
