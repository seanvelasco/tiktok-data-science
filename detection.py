import re
import difflib

LEETSPEAK_MAP = {
    '4': 'a', '3': 'e', '1': 'i', '0': 'o', '@': 'a', '$': 's',
    '7': 't', '5': 's', '+': 't'
}

def leetspeak_to_normal(text):
    return ''.join(LEETSPEAK_MAP.get(char.lower(), char.lower()) for char in text)

def detect_relevant_content(text, include_list, do_not_include_list=None, threshold=0.5):
    matches = []

    normalized_text = leetspeak_to_normal(text)

    for keyword in include_list:
        normalized_keyword = leetspeak_to_normal(keyword)

        if " " in keyword:
            if normalized_keyword in normalized_text:
                match = re.search(re.escape(keyword), text, re.IGNORECASE)
                if match:
                    matches.append(match.group(0))
        else:
            if normalized_keyword in normalized_text:
                match = re.search(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE)
                if match:
                    matches.append(match.group(0))
            else:
                match_ratio = difflib.SequenceMatcher(None, normalized_text, normalized_keyword).ratio()
                if match_ratio >= threshold:
                    match = re.search(re.escape(keyword), text, re.IGNORECASE)
                    if match:
                        matches.append(match.group(0))

    if do_not_include_list:
        for keyword in do_not_include_list:
            normalized_keyword = leetspeak_to_normal(keyword)

            if " " in keyword:
                if normalized_keyword in normalized_text:
                    match = re.search(re.escape(keyword), text, re.IGNORECASE)
                    if match:
                        matches = [m for m in matches if m != match.group(0)]  # Remove this match
            else:
                if normalized_keyword in normalized_text:
                    match = re.search(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE)
                    if match:
                        matches = [m for m in matches if m != match.group(0)]  # Remove this match
                else:
                    match_ratio = difflib.SequenceMatcher(None, normalized_text, normalized_keyword).ratio()
                    if match_ratio >= threshold:
                        match = re.search(re.escape(keyword), text, re.IGNORECASE)
                        if match:
                            matches = [m for m in matches if m != match.group(0)]  # Remove this match

    return matches
