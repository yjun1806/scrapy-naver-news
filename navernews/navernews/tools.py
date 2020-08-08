from w3lib.html import remove_tags, remove_tags_with_content, replace_entities, remove_comments


def clean_html(text):
    '''
    :param text:
    :return:
    Version : 2020-01-17_ver
    '''
    if text is not None:
        body = replace_entities(text)  # &nbsp; - 띄어쓰기, &#8216; - ... 이런것들 제거하는 코드
        # remove_tags : 입력된 텍스트에서 태그 제거하는 라이브러리 함수
        # remove_tags_with_content : 입력한 텍스트에서 선택된 태그안의 내용을 지우는 라이브러리 함수
        body = replace_entities(remove_tags_with_content(body, ('script', 'a', 'h4')))
        body = remove_comments(body)
        body = remove_tags(body)
        # body = re.sub('(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', '', body)  # 텍스트의 http url 제거
        # body = re.sub('([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', '', body)  # 텍스트의 이메일 제거
        # body = re.sub('[\{\}\[\]\/?;:|\)*~`!^\-_+<>@\#$%&\\\=\(]', ' ', body)  # 특수문자 제거
        # body = re.sub('([ㄱ-ㅎㅏ-ㅣ]+)', '', body)  # 한글 자음, 모음만 쓴것 제거
        body_split = body.split()  # 문자열을 리스트에 담는다.
        body = " ".join(body_split)
        return body
    else:
        return text
