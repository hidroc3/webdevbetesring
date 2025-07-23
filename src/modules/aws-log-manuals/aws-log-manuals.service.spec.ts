import { Test, TestingModule } from '@nestjs/testing';
import { AwsLogManualsService } from './aws-log-manuals.service';

describe('AwsLogManualsService', () => {
  let service: AwsLogManualsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AwsLogManualsService],
    }).compile();

    service = module.get<AwsLogManualsService>(AwsLogManualsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
