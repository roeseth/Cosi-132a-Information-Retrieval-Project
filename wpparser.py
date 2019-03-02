import wptools
import re


class wpParser:
    """
        A Wikipedia Parser using wpotools
    """
    title = None
    soup = None
    info = {}

    def __init__(self, title):
        p = wptools.page(title)
        self.title = title
        p.get_parse()
        tmpbox = p.data['infobox']
        if tmpbox is None:
            tmpbox = {}
        # Fill the info Dict while the parser being initialized
        # If info not available, fill blank
        self.info['director'] = self.parse_director(tmpbox['director']) if 'director' in tmpbox else ''
        self.info['starring'] = self.parse_sublist(tmpbox['starring']) if 'starring' in tmpbox else []
        if 'runtime' in tmpbox:
            time = self.parse_minutes(tmpbox['runtime'])
        elif 'duration' in tmpbox:
            time = self.parse_minutes(tmpbox['duration'])
        else:
            time = ''
        self.info['running time'] = time
        self.info['country'] = self.parse_sublist(tmpbox['country']) if 'country' in tmpbox else []
        self.info['language'] = self.parse_sublist(tmpbox['language']) if 'language' in tmpbox else ''

    @staticmethod
    def parse_sublist(str):
        """ To get a list of names from the messy string wptools returned

        :param str: the string from wptools
        :return: a list of names/locations
        """
        # Getting rid of invalid words and extract name if following the format
        # 'www', 'www www', 'www www www', 'w.', 'w. www', etc.
        return re.findall(r'\w{1,}\.?\s\w{1,}\.?\s\w{1,}\.?|\w{1,}\.?\s\w{1,}\.?|\w{4,}',
                          re.sub(r'Unbullted list|Plainlist|plainlist', '', str))

    @staticmethod
    def parse_director(str):
        """ To format the director name from the messy string wptools returned

        :param str: the string from wptools
        :return: a clean director name
        """
        # Getting rid of invalid characters and retrieve the first director name
        # Sometime wptools returns two names
        return re.split(r'<br>|\|', re.sub(r'\[{2}|\]{2}|\{{2}|\}{2}', '', str))[0]

    @staticmethod
    def parse_minutes(str):  # Getting the first number in the string and ignore ' minutes'
        patt = re.compile(r'\d+')
        time = patt.findall(str)
        t1 = int(time[0])
        t2 = 0
        if len(time) > 1 and t1 <= 3:
            t2 = int(time[1])
            return t1 * 60 + t2
        return t1
