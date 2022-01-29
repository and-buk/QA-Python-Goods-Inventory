TEST_CASES = [
    {"test_input": {'id': 1,
                    'name': 'Телевизор',
                    'package_params':
                        {'height': 5,
                         'width': 20},
                    'location_and_quantity': [{'location': 'Магазин на Ленина', 'amount': 7},
                                              {'location': 'Магазин в центре', 'amount': 3},
                                              {'location': 'Магазин на Верности', 'amount': 10}]},
     "expected": ((1, 'Телевизор', 5, 20),
                  ([1, 'Магазин на Ленина', 7],
                   [1, 'Магазин в центре', 3],
                   [1, 'Магазин на Верности', 10]))
     }
    ]

