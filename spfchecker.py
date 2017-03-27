#!/opt/python3/bin/python3.2


"""
This file takes a csv file and writes a new one that has the MX added.
"""

import concurrent.futures, optparse, csv, dns.resolver, os

parser = optparse.OptionParser('-i [input_file] -o [output_file]')
parser.add_option('-i', '--input', dest='input_file')
parser.add_option('-o', '--output', dest='output_file')

def parse_options():
    (options, args) = parser.parse_args()

    if not options.input_file:
        parser.error('An input file must be specified.')
    elif not options.output_file:
        options.output_file = os.path.join(os.path.dirname(__file__), "output.txt")
    elif options.input_file == options.output_file:
        parser.error('Input file and output file cannot be the same file.')
    #open options.input_file, file mode of r for reading only
    #and open output file (options.output_file) for reading to
    input_file = open(options.input_file, 'r')
    output_file = open(options.output_file, 'a+') #, encoding='utf-8')
    return input_file, output_file

def mx_from_csv(input_file):
    #mx = empty list
    mx = [] 

    def add_mx(future):
        mx.append(future.result())

    def fetch_mx(i, row):
        #created a variable, primaryMX equals none (python equivalent of null)
        primaryMX = None
        #variable domain equals the domain row, later defined as the first comma separated value on the domain_reader line
        domain = row['Domain']
        try:
        #created a variable, answers = dns.resolver.query(domain, 'MX'), using the dns library module to query for an mx record
            answers = dns.resolver.query(domain, 'MX')
            #created a variable best_answer = None
            best_answer = None
            #rdata is a variable made just only on the for rdata in answers line, being put in answers
            #for each answer called rdata, if best_answer or best_answer.preference is not less than rdata.preference, 
            #set best_answer to rdata and primaryMX to str(rdata.exchange)
            for rdata in answers:
                if not best_answer or best_answer.preference < rdata.preference:
                    best_answer = rdata
                    primaryMX = str(rdata.exchange)
            if dns.resolver.NoAnswer:
                primaryMX = "No answer"
            if (primaryMX.rfind('.') == len(primaryMX)-1):
                primaryMX = primaryMX[:-1]
            while(primaryMX.count('.')>1):
                primaryMX = primaryMX[primaryMX.find('.')+1:]
            row['MX'] = str.lower(primaryMX)
        except dns.resolver.NXDOMAIN:
            print("%d: NXDOMAIN on '%s'" % (i, domain))
        except dns.resolver.NoAnswer:
            print("%d: No answer on '%s'" % (i, domain))
        except dns.exception.Timeout:
            print("%d: Timeout on '%s'" % (i, domain))
        return row
# Website,Company Name,Corporate Phone,Address,City,State,Zip
    #dictReader is a file object and the second parameter on the following line are the fieldnames. So this is saying
    #the file you're reading which is called input_file, each input you receive is in the order of Domain, company, phone, etc...
    domain_reader = csv.DictReader(input_file, ['Domain','MX', 'Company', 'Phone', 'Address', 'City', 'State', 'Zip'])
    with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as e:
        for i, row in enumerate(domain_reader):
            e.submit(fetch_mx, i, row).add_done_callback(add_mx)
    return (mx)
    
def txt_from_mx(mx, output_file):
    writer = csv.DictWriter(output_file, ['Domain', 'MX', 'Company', 'Phone', 'Address', 'City', 'State', 'Zip'])
    writer.writerow({'City': 'City', 'Domain': 'Domain', 'Zip': 'Zip', 'Company': 'Company', 'Phone': 'Phone',
                     'State': 'State', 'Address': 'Address', 'MX': 'MX'})
    writer.writerows(mx)
    

if __name__ == '__main__':
    input_file, output_file = parse_options()
    mx = mx_from_csv(input_file)
    txt_from_mx(mx, output_file)
