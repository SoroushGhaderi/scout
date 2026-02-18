# FotMob browser cookies — single source of truth.
#
# Only `turnstile_verified` needs to be kept fresh (expires every ~1 hour).
# All other cookies are long-lived and rarely change.
#
# To refresh after a TURNSTILE_REQUIRED error:
#   1. Open Chrome and visit https://www.fotmob.com (wait for the page to load)
#   2. Run:  python3 scripts/refresh_turnstile.py
#
# The scraper hot-reloads this file on every request — no restart needed.

cookies = {
    '_ga': 'GA1.1.1885161742.1735855707',
    '_cc_id': 'eabe48f45af9438f46895c7eba9292c7',
    '_hjSessionUser_2585474': 'eyJpZCI6ImY4NmYxZmI2LTAwMDQtNTI5OC05MDVmLTY5MDVmOTY0YjMwZCIsImNyZWF0ZWQiOjE3MzU4NTU3MDk5NDMsImV4aXN0aW5nIjp0cnVlfQ==',
    '__eoi': 'ID=615792f41f13f107:T=1759952639:RT=1759952639:S=AA-AfjaeLa71U0CDZvd6eBGU_YRS',
    'cto_bundle': 'CZTt_19RZTh2MWdSaE11OFkyWk4zVDUlMkIlMkJXJTJCV2JyZk9ISSUyRk1iZ2ZmQ0c3SVM3MlZCcmRBY25XREJPcCUyRlBueDlCamVpZXVhb2U3NE1xOFI5MHhOelN4JTJGUWN2M2ZSUTR2V3JEVkJNRXh6RFE0JTJGbnBXRkxvUnRMTHRnJTJGcXZPTURzZzVPUjUxN1BQZWRHb0toYzZNTDJsSEpXejB6JTJCbkM5Z2ZSUURCekNSVnNQemxGZ25aampBWUhKVU1KRSUyRlJEY015dXVKVWFlcmowZXppSmVYbGRwenNWTzNWOHclM0QlM0Q',
    'FCNEC': '%5B%5B%22AKsRol85o_csinDqzdAaFCcMH-UnsbFcMTJwaQzvH2z8y0YRZn9JiMHGtWQmobIVwQK4NoLE4G0BmzSpz59gP9k8EJNyrowjTZe3oObnu1VtP4WS1FJUcGO9syQTiq2whXAzyHwWWcihxJE0HtqRzeQWXZYgzZCBfA%3D%3D%22%5D%5D',
    'u:location': '%7B%22countryCode%22%3A%22CA%22%2C%22regionId%22%3A%22AB%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22ccode3%22%3A%22CAN_AB%22%2C%22ccode3NoRegion%22%3A%22CAN%22%2C%22timezone%22%3A%22Asia%2FTehran%22%7D',
    '_gcl_au': '1.1.1108002159.1771309919',
    '_ga_G0V1WDW9B2': 'GS2.1.s1771396894$o53$g0$t1771397134$j60$l0$h1879018887',
    'g_state': '{"i_l":0,"i_ll":1771412252165,"i_b":"stt1yNafZed3bkog2Y73qU53g+P4sXu+5JLmWa5SLhE","i_e":{"enable_itp_optimization":0}}',
    'turnstile_verified': '1.1771434664.35dbc3be2d3c361591bbe2decee49115f6af6b1fe12dd8b2fb167c919013cbaa',
}
