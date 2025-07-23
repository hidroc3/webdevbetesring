import { Test, TestingModule } from '@nestjs/testing';
import { ArrLogsService } from './arr-logs.service';

describe('ArrLogsService', () => {
  let service: ArrLogsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ArrLogsService],
    }).compile();

    service = module.get<ArrLogsService>(ArrLogsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
