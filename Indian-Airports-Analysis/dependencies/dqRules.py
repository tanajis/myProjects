def isNUll(value):
    """
    return True if given column is Null.
    """
    if type(value) == type(''):
        value = value.trim()
    if value == '' or value is None or value is NoneType() or value = '':
        return True 
    else:
        return False

def isInt(value):
    """
    return True if given column is Integer.
    """
    if type(value) == IntegerType():
        return True
    else:
        return False


def isFloat(value):
    """
    return True if given column is Float.
    """
    if type(value) == FloatType():
        return True
    else:
        return False

def DQNull(df:dataframe,columnsList):
    """
    return True if given column is Float.
    """

#lat long should not be Null,''Must be float
# trim the strings
# No special character
# String should not have ''
# good data and bad data
# Find duplicates
# 
# #https://medium.com/@cprosenjit/implementing-data-quality-with-amazon-deequ-apache-spark-adcdf7c0a8da
# 
# isUnique
# isPrimaryKey
# hasSize
# hasMinLength
# hasDataType
# hasMaxLength
# isPositive
# isNonNegative
# isLessThan
# isLessThanOrEqualTo
# isGreaterThan
# isGreaterThanOrEqualTo
# isContainedIn    
# hasPattern
