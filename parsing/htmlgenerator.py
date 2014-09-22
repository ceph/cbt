import base64
import zlib
import urllib

class HTMLGenerator(object):
    def __init__(self):
        self.styles = []
        self.html = []
        self.scripts = []

    def encode(self, text):
        return base64.b64encode(zlib.compress(urllib.quote(text), 9))

    def read_file(self, filename):
        f = open(filename, "r")
        text = f.read()
        f.close()
        return text

    def add_html(self, text):
        self.html.append(text)

    def add_style(self, text):
        self.styles.append('<style type="text/css">\n' + text + '</style>')

    def add_script(self, text):
        self.scripts.append('<script type="text/javascript">\n' + text + '\n</script>')

    def add_encoded_script(self, text):
        self.scripts.append('<script type="text/javascript">\n  eval(JXG.decompress("' + self.encode(text) + '"));\n</script>')

    def to_string(self):
        return '\n'.join(self.html + self.styles + self.scripts)

    def format_data(self, data):
        lines = []
        for row in data:
            tmprow = [] 
            for pair in row:
                first = '"' + pair[0] + '"'
                second = '' 
                if isinstance(pair[1], float):
                    second = "%.2f" % pair[1]
                elif isinstance(pair[1], str) or isinstance(pair[1], unicode):
                    second = '"' + pair[1] + '"'
                else:
                    second = pair[1]
                tmprow.append(str(first) + ':' + str(second))
            lines.append('{' + ', '.join(tmprow) + '}')
        return 'var dataSet = [' + ',\n'.join(lines) + '];\n'
