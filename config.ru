require 'rubygems'
require 'rack'
require 'csv'
require 'tiny_tds'

app = proc do |env|
  human_resources = TinyTds::Client.new(
    :username => '', 
    :password => '', 
    :host => '', 
    :database => ''
  )

  common_login = TinyTds::Client.new(
    :username => '', 
    :password => '', 
    :host => '', 
    :database => ''
  )

  body = ""
  CSV.foreach("/home/adam/positions.csv", {:headers => true}) do |row|
    title = row['title'].to_s.downcase.split(' ').map {|w| w.capitalize }.join(' ')
    title.gsub!(/Mgr\./, 'Manager')
    title.gsub!(/Mgt\./, 'Management')
    title.gsub!(/Magmt/, 'Management')

    group = row['classification'].to_s.split(' ')[0]
    level = row['classification'].to_s.split(' ')[1]

    tenure = row['tenure'].to_s.split(' ')[0].to_s.capitalize
    tenure.gsub!(/Fswep/, 'Student')
    tenure.gsub!(/Coop/, 'Student')
    tenure.gsub!(/Part-time/, 'Casual')
    tenure.gsub!(/Casual\/occasionnel/, 'Casual')

    location = row['location'].to_s.capitalize

    language_consideration = row['language'].to_s
    language_consideration.gsub!(/BBBBBB/, 'Bilingual BBB')
    language_consideration.gsub!(/^BBB/, 'Bilingual BBB')
    language_consideration.gsub!(/CBCCBC/, 'Bilingual CBC')
    language_consideration.gsub!(/Eng\/FRE/, 'Bilingual BBB')
    language_consideration.gsub!(/Eng/, 'English Essential')
    language_consideration.gsub!(/Fre/, 'French Essential')

    number = row['number'].to_s
    number.gsub!(/TO BE CREATED/, '')

    orgunit = row['orgunit'].to_s
    orgunit.gsub!(/'/, '')
    orgunit_abbr = orgunit.gsub(/[a-z\s]/, '')
    orgunit_abbr.gsub!(/ITS-A/, 'ITS-ACQ')
    orgunit_abbr.gsub!(/ITS-C/, 'ITS-COMP')
    orgunit_abbr.gsub!(/ITS-PLANS&PERFORMANCEMANAGEMENT/, 'ITS-PPM')
    orgunit_abbr.gsub!(/ITSP&PM/, 'ITS-PPM')
    orgunit_abbr.gsub!(/ITINFRASTRUCTURESERVICES/, 'ITIS')
    orgunit_abbr = 'IOSS' if orgunit_abbr.nil? || orgunit_abbr.empty?

    next if group.nil? || group.empty?
    next if level.nil? || level.empty?
    next if title.nil? || title.empty?
    next if tenure.nil? || tenure.empty?
    next if location.nil? || location.empty?

    sql = <<-SQL
      SELECT id
      FROM classifications
      WHERE name = '#{group}'
    SQL
    result = human_resources.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      result.cancel
      sql = <<-SQL
        INSERT INTO classifications (name)
        VALUES ('#{group}')
      SQL
      result = human_resources.execute(sql)
      classification_id = result.insert
    else
      classification_id = first['id']
    end
    result.cancel

    sql = <<-SQL
     SELECT id FROM classification_levels
     WHERE name = '#{level}'
     AND classification_id = #{classification_id}
    SQL
    result = human_resources.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      result.cancel
      sql = <<-SQL
        INSERT INTO classification_levels (name, classification_id)
        VALUES ('#{level}', #{classification_id})
      SQL
      result = human_resources.execute(sql)
      classification_level_id = result.insert
    else
      classification_level_id = first['id']
    end
    result.cancel

    sql = <<-SQL
     SELECT id FROM branches 
     WHERE name = '#{orgunit}'
    SQL
    result = common_login.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      sql = <<-SQL
        INSERT INTO branches (name, acronym)
        SELECT '#{orgunit}', '#{orgunit_abbr}'
      SQL
      result = common_login.execute(sql)
      branch_id = result.insert
    else
      branch_id = first['id']
    end
    result.cancel

    sql = <<-SQL
     SELECT id FROM jobs
     WHERE title = '#{title}'
     AND classification_level_id = #{classification_level_id}
     AND branch = '#{orgunit_abbr}'
    SQL
    result = human_resources.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      sql = <<-SQL
        INSERT INTO jobs (title, classification_level_id, branch)
        VALUES ('#{title}', #{classification_level_id}, '#{orgunit_abbr}')
      SQL
      result = human_resources.execute(sql)
      result.insert
      result.cancel

      sql = <<-SQL
       SELECT id FROM jobs
       WHERE title = '#{title}'
       AND classification_level_id = #{classification_level_id}
       AND branch = '#{orgunit_abbr}'
      SQL
      result = human_resources.execute(sql)
      result.each
      first = result.first
    end
    job_id = first['id']
    result.cancel

    sql = <<-SQL
     SELECT id FROM tenures
     WHERE name = '#{tenure}'
    SQL
    result = human_resources.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      result.cancel
      sql = <<-SQL
        INSERT INTO tenures (name)
        VALUES ('#{tenure}')
      SQL
      result = human_resources.execute(sql)
      tenure_id = result.insert
    else
      tenure_id = first['id']
    end
    result.cancel

    sql = <<-SQL
     SELECT id FROM language_considerations
     WHERE name = '#{language_consideration}'
    SQL
    result = human_resources.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      result.cancel
      sql = <<-SQL
        INSERT INTO language_considerations (name)
        VALUES ('#{language_consideration}')
      SQL
      result = human_resources.execute(sql)
      language_consideration_id = result.insert
    else
      language_consideration_id = first['id']
    end
    result.cancel

    sql = <<-SQL
     SELECT id FROM locations
     WHERE name = '#{location}'
    SQL
    result = common_login.execute(sql)
    result.each
    first = result.first
    result.cancel

    if first.nil?
      result.cancel
      sql = <<-SQL
        INSERT INTO locations (region_id, name)
        SELECT 6, '#{location}'
      SQL
      result = common_login.execute(sql)
      location_id = result.insert
    else
      location_id = first['id']
    end
    result.cancel

    sql = <<-SQL
      INSERT INTO positions (
        job_id, 
        security_level_id,
        tenure_id,
        manager_id,
        location,
        language_consideration_id,
        number,
        fiscal_year)
      VALUES (
        #{job_id},
        1,
        #{tenure_id},
        97,
        '#{location}',
        #{language_consideration_id},
        '#{number}',
        '2011'
      )
    SQL
    result = human_resources.execute(sql)
    result.cancel

   body += "#{job_id} #{title} #{group}-#{level} #{tenure} #{location} #{language_consideration} #{classification_id} #{classification_level_id}<br />"
  end

  [200, {'Content-Type' => 'text/html', 'Content-Length' => body.length.to_s}, [body]]
end

run app
