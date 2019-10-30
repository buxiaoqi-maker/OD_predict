# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 14:34:19 2019

@author: 46573
"""

import OD_shuaka
import OD_erweima
import OD_jinrongka
import datetime

def main(time):
    OD_shuaka.main(time)
    OD_jinrongka.main(time)
    OD_erweima.main(time)



if __name__ == '__main__':
    
    '''获取上个月月份'''
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    time = last_month.strftime("%Y-%m")
    main(time)